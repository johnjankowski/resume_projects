package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;


public class GraceHashOperator extends JoinOperator {

    private int numBuffers;

    public GraceHashOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.GRACEHASH);

        this.numBuffers = transaction.getNumMemoryPages();
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new GraceHashIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        TableStats leftStats = this.getLeftSource().getStats();
        TableStats rightStats = this.getRightSource().getStats();
        int rightPages = rightStats.getNumPages();
        int leftPages = leftStats.getNumPages();
        return 3 * (rightPages + leftPages);
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class GraceHashIterator implements Iterator<Record> {
        private Iterator<Record> leftIterator;
        private Iterator<Record> rightIterator;
        private Record rightRecord;
        private Record nextRecord;
        private String[] leftPartitions;
        private String[] rightPartitions;
        private int currentPartition;
        private Map<DataType, ArrayList<Record>> inMemoryHashTable;
        private int leftColumnIndex;
        private int rightColumnIndex;
        private int arrayListIndex;


        public GraceHashIterator() throws QueryPlanException, DatabaseException {
            this.leftIterator = getLeftSource().iterator();
            this.rightIterator = getRightSource().iterator();
            this.leftPartitions = new String[numBuffers - 1];
            this.rightPartitions = new String[numBuffers - 1];
            String leftTableName;
            String rightTableName;
            for (int i = 0; i < numBuffers - 1; i++) {
                leftTableName = "Temp HashJoin Left Partition " + Integer.toString(i);
                rightTableName = "Temp HashJoin Right Partition " + Integer.toString(i);
                GraceHashOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTableName);
                GraceHashOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTableName);
                this.leftPartitions[i] = leftTableName;
                this.rightPartitions[i] = rightTableName;
            }
            this.leftColumnIndex = GraceHashOperator.this.getLeftColumnIndex();
            while (this.leftIterator.hasNext()) {
                Record curr_record = leftIterator.next();
                DataType join_key_val = curr_record.getValues().get(leftColumnIndex);
                int hashed_val = join_key_val.hashCode() % (numBuffers - 1);
                GraceHashOperator.this.addRecord(this.leftPartitions[hashed_val], curr_record.getValues());
            }

            this.rightColumnIndex = GraceHashOperator.this.getRightColumnIndex();
            while (this.rightIterator.hasNext()) {
                Record curr_record = rightIterator.next();
                DataType join_key_val = curr_record.getValues().get(rightColumnIndex);
                int hashed_val = join_key_val.hashCode() % (numBuffers - 1);
                GraceHashOperator.this.addRecord(this.rightPartitions[hashed_val], curr_record.getValues());
            }
            this.rightIterator = GraceHashOperator.this.getTableIterator(this.rightPartitions[0]);
            this.leftIterator = GraceHashOperator.this.getTableIterator(this.leftPartitions[0]);
            this.inMemoryHashTable = new HashMap<>();
            while (this.leftIterator.hasNext()) {
                Record leftRecord = this.leftIterator.next();
                ArrayList<Record> leftRecordList = this.inMemoryHashTable.get(leftRecord.getValues().get(this.leftColumnIndex));
                if (leftRecordList == null) {
                    leftRecordList = new ArrayList<>();
                }
                leftRecordList.add(leftRecord);
                this.inMemoryHashTable.put(leftRecord.getValues().get(this.leftColumnIndex), leftRecordList);
            }
            this.rightRecord = null;
            this.nextRecord = null;
            this.currentPartition = 0;
            this.arrayListIndex = 0;

        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        public boolean hasNext() {
            if (this.nextRecord != null) {
                return true;
            }
            while (true) {
                if (currentPartition == numBuffers - 1) {
                    return false;
                }
                // if right record is null, we need to get next one in current partition, if no more then we increment partition
                else if (this.rightRecord == null) {
                    if (this.rightIterator.hasNext()) {
                        this.rightRecord = this.rightIterator.next();
                    } else if (this.currentPartition < numBuffers - 2) { //we go here once we reach the end of a partition
                        this.currentPartition++;
                        this.inMemoryHashTable = new HashMap<>();
                        try {
                            this.leftIterator = GraceHashOperator.this.getTableIterator(this.leftPartitions[this.currentPartition]);
                            this.rightIterator = GraceHashOperator.this.getTableIterator(this.rightPartitions[this.currentPartition]);
                        } catch (DatabaseException e) {
                            return false;
                        }
                        while (this.leftIterator.hasNext()) {
                            Record leftRecord = this.leftIterator.next();
                            ArrayList<Record> leftRecordList = this.inMemoryHashTable.get(leftRecord.getValues().get(this.leftColumnIndex));
                            if (leftRecordList == null) {
                                leftRecordList = new ArrayList<>();
                            }
                            leftRecordList.add(leftRecord);
                            this.inMemoryHashTable.put(leftRecord.getValues().get(this.leftColumnIndex), leftRecordList);
                        }
                    } else {
                        this.currentPartition++;
                    }
                }
                // probe with right record
                else {
                    DataType rightValue = this.rightRecord.getValues().get(this.rightColumnIndex);
                    if (this.inMemoryHashTable.containsKey(rightValue)) {
                        ArrayList<Record> leftRecords = this.inMemoryHashTable.get(rightValue);
                        if (this.arrayListIndex < leftRecords.size()) {
                            Record leftRecord = leftRecords.get(this.arrayListIndex);
                            this.arrayListIndex++;
                            List<DataType> leftValues = new ArrayList<DataType>(leftRecord.getValues());
                            List<DataType> rightValues = new ArrayList<DataType>(this.rightRecord.getValues());
                            leftValues.addAll(rightValues);
                            this.nextRecord = new Record(leftValues);
                            return true;
                        } else {
                            this.rightRecord = null;
                            this.arrayListIndex = 0;
                        }
                    }
                }
            }
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
