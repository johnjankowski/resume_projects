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
import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class PNLJOperator extends JoinOperator {

    public PNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.PNLJ);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new PNLJIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        TableStats leftStats = this.getLeftSource().getStats();
        TableStats rightStats = this.getRightSource().getStats();
        int rightPages = rightStats.getNumPages();
        int leftPages = leftStats.getNumPages();
        return leftPages * rightPages + leftPages;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class PNLJIterator implements Iterator<Record> {
        private String leftTableName;
        private String rightTableName;
        private Iterator<Page> leftIterator;
        private Iterator<Page> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private Page leftPage;
        private Page rightPage;
        private byte[] leftPageHeader;
        private byte[] rightPageHeader;
        private int leftPageIndex;
        private int leftPageHeaderSize;
        private int leftEntrySize;
        private int rightPageIndex;
        private int rightPageHeaderSize;
        private int rightEntrySize;


        public PNLJIterator() throws QueryPlanException, DatabaseException {
            if (PNLJOperator.this.getLeftSource().isSequentialScan()) {
                this.leftTableName = ((SequentialScanOperator) PNLJOperator.this.getLeftSource()).getTableName();
            } else {
                this.leftTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getLeftColumnName() + "Left";
                PNLJOperator.this.createTempTable(PNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
                Iterator<Record> leftIter = PNLJOperator.this.getLeftSource().iterator();
                while (leftIter.hasNext()) {
                    PNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
                }
            }

            if (PNLJOperator.this.getRightSource().isSequentialScan()) {
                this.rightTableName = ((SequentialScanOperator) PNLJOperator.this.getRightSource()).getTableName();
            } else {
                this.rightTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getRightColumnName() + "Right";
                PNLJOperator.this.createTempTable(PNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
                Iterator<Record> rightIter = PNLJOperator.this.getRightSource().iterator();
                while (rightIter.hasNext()) {
                    PNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
                }
            }

            // TODO: implement me!

            this.leftIterator = PNLJOperator.this.getPageIterator(leftTableName);
            leftIterator.next();
            this.rightIterator = null;
            this.leftRecord = null;
            this.nextRecord = null;
            this.rightRecord = null;
            this.leftPage = null;
            this.leftPageHeader = null;
            this.leftPageIndex = 0;
            this.leftPageHeaderSize = PNLJOperator.this.getHeaderSize(this.leftTableName);
            this.leftEntrySize = PNLJOperator.this.getEntrySize(this.leftTableName);
            this.rightPage = null;
            this.rightPageHeader = null;
            this.rightPageIndex = 0;
            this.rightPageHeaderSize = PNLJOperator.this.getHeaderSize(this.rightTableName);
            this.rightEntrySize = PNLJOperator.this.getEntrySize(this.rightTableName);
        }

        public boolean hasNext() {

            if (this.nextRecord != null) {
                return true;
            }

            while (true) {
                // left page becomes null if every record on it has been compared to every record in right table
                // if left page is null then get the next page and create new right iterator
                if (this.leftPage == null) {
                    if (leftIterator.hasNext()) {
                        this.leftPage = leftIterator.next();
                        try {
                            this.leftPageHeader = PNLJOperator.this.getPageHeader(this.leftTableName, this.leftPage);
                            this.rightIterator = PNLJOperator.this.getPageIterator(this.rightTableName);
                            this.rightIterator.next();
                        } catch (DatabaseException e) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                // right page becomes null if every left record on page has been compared to every right record on page
                // if right page is null, get the next one if there is one, else set left page to null
                else if (this.rightPage == null) {
                    if (this.rightIterator.hasNext()) {
                        this.rightPage = this.rightIterator.next();
                        try {
                            this.rightPageHeader = PNLJOperator.this.getPageHeader(this.rightTableName, this.rightPage);
                        } catch (DatabaseException e) {
                            return false;
                        }
                    } else {
                        this.leftPage = null;
                    }
                }
                // left record becomes null if its been compared to every record on right page
                // if left record is null, get the next one if there is, else set right page to null and reset left index
                else if (this.leftRecord == null) {
                    int slot = this.getNextFilledSlotNum(this.leftPageIndex, this.leftPageHeader);
                    if (slot >= 0) {
                        this.leftRecord = this.getRecord(slot, this.leftPage, "left");
                    } else {
                        this.rightPage = null;
                        this.leftPageIndex = 0;
                    }
                }
                else {
                    int slot = this.getNextFilledSlotNum(this.rightPageIndex, this.rightPageHeader);
                    while (slot >= 0) {
                        this.rightRecord = this.getRecord(slot, this.rightPage, "right");
                        DataType leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
                        DataType rightJoinValue = this.rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());

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
                return PNLJOperator.this.getLeftSource().getOutputSchema().decode(encoded_record);
            } else {
                this.rightPageIndex = slot + 1;
                int offset = this.rightPageHeaderSize + slot * this.rightEntrySize;
                byte[] encoded_record = cur_page.readBytes(offset, this.rightEntrySize);
                return PNLJOperator.this.getRightSource().getOutputSchema().decode(encoded_record);
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
