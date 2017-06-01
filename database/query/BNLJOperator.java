package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class BNLJOperator extends JoinOperator {

    private int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getNumMemoryPages();
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new BNLJIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        TableStats leftStats = this.getLeftSource().getStats();
        TableStats rightStats = this.getRightSource().getStats();
        int rightPages = rightStats.getNumPages();
        double leftPages = leftStats.getNumPages();
        return (int) (Math.ceil(leftPages / (this.numBuffers - 2)) * rightPages + leftPages);
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class BNLJIterator implements Iterator<Record> {
        private String leftTableName;
        private String rightTableName;
        private Iterator<Page> leftIterator;
        private Iterator<Page> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private Page leftPage;
        private Page rightPage;
        private Page[] block;
        private byte[] leftPageHeader;
        private byte[] rightPageHeader;
        private int leftPageIndex;
        private int leftPageHeaderSize;
        private int leftEntrySize;
        private int rightPageIndex;
        private int rightPageHeaderSize;
        private int rightEntrySize;
        private int block_size;
        private int currBlockIndex;

        public BNLJIterator() throws QueryPlanException, DatabaseException {
            if (BNLJOperator.this.getLeftSource().isSequentialScan()) {
                this.leftTableName = ((SequentialScanOperator) BNLJOperator.this.getLeftSource()).getTableName();
            } else {
                this.leftTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getLeftColumnName() + "Left";
                BNLJOperator.this.createTempTable(BNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
                Iterator<Record> leftIter = BNLJOperator.this.getLeftSource().iterator();
                while (leftIter.hasNext()) {
                    BNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
                }
            }
            if (BNLJOperator.this.getRightSource().isSequentialScan()) {
                this.rightTableName = ((SequentialScanOperator) BNLJOperator.this.getRightSource()).getTableName();
            } else {
                this.rightTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getRightColumnName() + "Right";
                BNLJOperator.this.createTempTable(BNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
                Iterator<Record> rightIter = BNLJOperator.this.getRightSource().iterator();
                while (rightIter.hasNext()) {
                    BNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
                }
            }

            // TODO: implement me!

            this.leftIterator = BNLJOperator.this.getPageIterator(leftTableName);
            leftIterator.next();
            this.block_size = BNLJOperator.this.numBuffers - 2;
            this.block = null;
            this.currBlockIndex = 0;
            this.rightIterator = null;
            this.leftRecord = null;
            this.nextRecord = null;
            this.rightRecord = null;
            this.leftPage = null;
            this.leftPageHeader = null;
            this.leftPageIndex = 0;
            this.leftPageHeaderSize = BNLJOperator.this.getHeaderSize(this.leftTableName);
            this.leftEntrySize = BNLJOperator.this.getEntrySize(this.leftTableName);
            this.rightPage = null;
            this.rightPageHeader = null;
            this.rightPageIndex = 0;
            this.rightPageHeaderSize = BNLJOperator.this.getHeaderSize(this.rightTableName);
            this.rightEntrySize = BNLJOperator.this.getEntrySize(this.rightTableName);

        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        public boolean hasNext() {
            // TODO: implement me!
            if (this.nextRecord != null) {
                return true;
            }

            while (true) {
                //block is null if every left record in block has been compared to right table
                if (this.block == null) {
                    this.block = new Page[block_size];
                    int i = 0;
                    while ((i < this.block_size) && (this.leftIterator.hasNext())) {
                        this.block[i] = this.leftIterator.next();
                        i++;
                    }
                    this.leftPage = this.block[0];
                    if (this.leftPage == null) {
                        return false;
                    }
                    try {
                        this.leftPageHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
                        this.rightIterator = BNLJOperator.this.getPageIterator(this.rightTableName);
                        this.rightIterator.next();
                    } catch (DatabaseException e) {
                        return false;
                    }
                }
                // right page becomes null if every left record in block has been compared to every right record on page
                // if right page is null, get the next one if there is one, else set left page to null
                else if (this.rightPage == null) {
                    if (this.rightIterator.hasNext()) {
                        this.rightPage = this.rightIterator.next();
                        try {
                            this.rightPageHeader = BNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
                        } catch (DatabaseException e) {
                            return false;
                        }
                    } else {
                        this.block = null;
                    }
                }
                //left page is null if every record on it has been compared to every right record on right page
                //check if any more pages in block, and advance to next if there is, else reset block counter and set right page to null
                else if (this.leftPage == null) {
                    if (currBlockIndex < block_size - 1) {
                        currBlockIndex++;
                        this.leftPage = this.block[this.currBlockIndex];
                        if (this.leftPage == null) {
                            this.currBlockIndex = 0;
                            this.leftPage = this.block[this.currBlockIndex];
                            this.rightPage = null;
                        }
                        try {
                            this.leftPageHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
                        } catch (DatabaseException e) {
                            return false;
                        }
                    } else {
                        this.currBlockIndex = 0;
                        this.leftPage = this.block[this.currBlockIndex];
                        this.rightPage = null;
                        try {
                            this.leftPageHeader = BNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
                        } catch (DatabaseException e) {
                            return false;
                        }
                    }
                }
                // left record becomes null if its been compared to every record on right page
                // if left record is null, get the next one if there is, else set right page to null and reset left index
                else if (this.leftRecord == null) {
                    int slot = this.getNextFilledSlotNum(this.leftPageIndex, this.leftPageHeader);
                    if (slot >= 0) {
                        this.leftRecord = this.getRecord(slot, this.leftPage, "left");
                    } else {
                        this.leftPage = null;
                        this.leftPageIndex = 0;
                    }
                }
                else {
                    int slot = this.getNextFilledSlotNum(this.rightPageIndex, this.rightPageHeader);
                    while (slot >= 0) {
                        this.rightRecord = this.getRecord(slot, this.rightPage, "right");
                        DataType leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                        DataType rightJoinValue = this.rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());

                        if (leftJoinValue.equals(rightJoinValue)) {
                            List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues());
                            List<DataType> rightValues = new ArrayList<DataType>(this.rightRecord.getValues());

                            leftValues.addAll(rightValues);
                            this.nextRecord = new Record(leftValues);
                            return true;
                        }
                        slot = this.getNextFilledSlotNum(this.rightPageIndex, this.rightPageHeader);
                    }
                    this.leftRecord = null;
                    this.rightPageIndex = 0;
                }
            }
        }

        /**
         * gets the next record on this page starting from given index
         * so get record(0, page, "left") will get the first record on the given left page
         */
        private Record getRecord(int slot, Page cur_page, String side) {
            if (side == "left") {
                this.leftPageIndex = slot + 1;
                int offset = this.leftPageHeaderSize + slot * this.leftEntrySize;
                byte[] encoded_record = cur_page.readBytes(offset, this.leftEntrySize);
                return BNLJOperator.this.getLeftSource().getOutputSchema().decode(encoded_record);
            } else {
                this.rightPageIndex = slot + 1;
                int offset = this.rightPageHeaderSize + slot * this.rightEntrySize;
                byte[] encoded_record = cur_page.readBytes(offset, this.rightEntrySize);
                return BNLJOperator.this.getRightSource().getOutputSchema().decode(encoded_record);
            }
        }


        /**
         * @param start slot number to start checking at
         * @param page_header page header to check for records
         * @return next valid slot number or -1 if there are none
         */
        private int getNextFilledSlotNum(int start, byte[] page_header) {
            int count = 0;
            for (byte b : page_header) {
                for (int mask = 0x80; mask != 0x00; mask >>>= 1) {
                    if ((b & (byte) mask) == (byte) mask) {
                        if (count >= start) {
                            return count;
                        }
                    }
                    count++;
                }
            }
            return -1;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public Record next() {
            if (this.hasNext()) {
                Record r = this.nextRecord;
                this.nextRecord = null;
                return r;
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
