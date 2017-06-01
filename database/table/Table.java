package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.BoolDataType;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.datatypes.FloatDataType;
import edu.berkeley.cs186.database.datatypes.IntDataType;
import edu.berkeley.cs186.database.datatypes.StringDataType;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import java.util.NoSuchElementException;
import java.util.Iterator;
import java.io.Closeable;

/**
 * A database table. Allows the user to add, delete, update, and get records.
 * A table has an associated schema, stats, and page allocator. The first page
 * in the page allocator is a header page that serializes the schema, and each
 * subsequent page is a data page containing the table records.
 *
 * Properties:
 * `schema`: the Schema (column names and column types) for this table
 * `freePages`: a set of page numbers that correspond to allocated pages with free space
 * `stats`: the TableStats for this table
 * `allocator`: the PageAllocator for this table
 * `tableName`: name of this table
 * `numEntriesPerPage`: number of records a data page of this table can hold
 * `pageHeaderSize`: physical size (in bytes) of a page header slot bitmap
 * `numRecords`: number of records currently contained in this table
 */
public class Table implements Iterable<Record>, Closeable {
  public static final String FILENAME_PREFIX = "db";
  public static final String FILENAME_EXTENSION = ".table";

  private Schema schema;
  private TreeSet<Integer> freePages;

  private TableStats stats;

  private PageAllocator allocator;
  private String tableName;

  private int numEntriesPerPage;
  private int pageHeaderSize;
  private long numRecords;

  public Table(String tableName) {
    this(tableName, FILENAME_PREFIX);
  }

  public Table(String tableName, String filenamePrefix) {
    this.tableName = tableName;

    String pathname = Paths.get(filenamePrefix, tableName + FILENAME_EXTENSION).toString();
    this.allocator = new PageAllocator(pathname, false);
    this.readHeaderPage();

    this.stats = new TableStats(this.schema);

    this.freePages = new TreeSet<Integer>();
    this.setEntryCounts();
    Iterator<Page> pIter = this.allocator.iterator();
    pIter.next();

    long freshCountRecords = 0;

    while(pIter.hasNext()) {
      Page p = pIter.next();

      // add all records in this page to TableStats
      int entryNum = 0;
      byte[] header = this.readPageHeader(p);
      while (entryNum < this.numEntriesPerPage) {
        byte b = header[entryNum/8];
        int bitOffset = 7 - (entryNum % 8);
        byte mask = (byte) (1 << bitOffset);

        byte value = (byte) (b & mask);
        if (value != 0) {
          int entrySize = this.schema.getEntrySize();

          int offset = this.pageHeaderSize + (entrySize * entryNum);
          byte[] bytes = p.readBytes(offset, entrySize);

          Record record = this.schema.decode(bytes);
          entryNum++;

          this.stats.addRecord(record);
        }

        entryNum++;
      }

      if (spaceOnPage(p)) {
        this.freePages.add(p.getPageNum());
      }

      freshCountRecords += numValidEntries(p);
    }

    this.numRecords = freshCountRecords;
  }

  public Table(Schema schema, String tableName) {
    this(schema, tableName, FILENAME_PREFIX);
  }

  /**
   * This constructor is used for creating a table in some specified directory.
   *
   * @param schema the schema for this table
   * @param tableName the name of the table
   * @param filenamePrefix the prefix where the table's files will be created
   */
  public Table(Schema schema, String tableName, String filenamePrefix) {
    this.schema = schema;
    this.tableName = tableName;
    this.stats = new TableStats(this.schema);

    this.freePages = new TreeSet<Integer>();
    String pathname = Paths.get(filenamePrefix, tableName + FILENAME_EXTENSION).toString();
    this.allocator = new PageAllocator(pathname, true);

    this.setEntryCounts();

    this.writeHeaderPage();
  }

  public void close() {
    allocator.close();
  }

  public Iterator<Record> iterator() {
      return new TableIterator();
  }

  public Iterator<Page> pageIterator() {
    return this.allocator.iterator();
  }

  /**
   * Add a new record to this table. The record should be added to the first
   * free slot of the first free page if one exists, otherwise a new page should
   * be allocated and the record should be placed in the first slot of that
   * page. Recall that a free slot in the slot bitmap means the bit is set to 0.
   * Make sure to update this.stats, this.freePages, and this.numRecords as
   * necessary.
   *
   * @param values the values of the record being added
   * @return the RecordID of the added record
   * @throws DatabaseException if the values passed in to this method do not
   *         correspond to the schema of this table
   */
  public RecordID addRecord(List<DataType> values) throws DatabaseException {
    Page cur_page;
    int page_num;
    int offset;
    int slot_number = 0;
    if (this.freePages.isEmpty()) {
      offset = this.pageHeaderSize;
      page_num = this.allocator.allocPage();
      cur_page = this.allocator.fetchPage(page_num);
      this.writeBitToHeader(cur_page, 0, (byte) 1);
    }
    else {
      page_num = this.freePages.pollFirst();
      cur_page = this.allocator.fetchPage(page_num);
      byte[] page_header = this.readPageHeader(cur_page);
      slot_number = this.getNextFreeSlotNum(0, page_header);
      if (slot_number == -1) {
        System.out.print("Something went wrong, no slots are free in a page with free slots");
      }
      this.writeBitToHeader(cur_page, slot_number, (byte) 1);
      offset = this.pageHeaderSize + slot_number * schema.getEntrySize();
    }
    try {
      Record record_values = this.schema.verify(values);
      byte[] byte_values = this.schema.encode(record_values);
      cur_page.writeBytes(offset, byte_values.length, byte_values);
      this.stats.addRecord(record_values);
    } catch (SchemaException e) {
      throw new DatabaseException("values passed in do not correspond to the schema of this table");
    }
    if (spaceOnPage(cur_page)) {
      this.freePages.add(page_num);
    }
    this.numRecords += 1;
    return new RecordID(page_num, slot_number);
  }

  /**
   *
   * @param start the first slot to check
   * @param page_header the page header to check
   * @return next free slot number or -1 if there isn't one
   */
  private int getNextFreeSlotNum(int start, byte[] page_header) {
    int count = 0;
    for (byte b : page_header) {
      for (int mask = 0x80; mask != 0x00; mask >>>= 1) {
        if ((b & (byte) mask) == 0) {
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
   * Deletes the record specified by rid from the table. Make sure to update
   * this.stats, this.freePages, and this.numRecords as necessary.
   *
   * @param rid the RecordID of the record to delete
   * @return the Record referenced by rid that was removed
   * @throws DatabaseException if rid does not correspond to a valid record
   */
  public Record deleteRecord(RecordID rid) throws DatabaseException {
    Record old_record = this.getRecord(rid);
    writeBitToHeader(this.allocator.fetchPage(rid.getPageNum()), rid.getSlotNumber(), (byte) 0);
    this.stats.removeRecord(old_record);
    this.numRecords -= 1;
    this.freePages.add(rid.getPageNum());
    return old_record;
  }

  /**
   * Retrieves a record from the table.
   *
   * @param rid the RecordID of the record to retrieve
   * @return the Record referenced by rid
   * @throws DatabaseException if rid does not correspond to a valid record
   */
  public Record getRecord(RecordID rid) throws DatabaseException {
    if (!this.checkRecordIDValidity(rid)) {
      throw new DatabaseException("rid does not correspond to a valid record");
    }
    Page cur_page = this.allocator.fetchPage(rid.getPageNum());
    int offset = this.pageHeaderSize + rid.getSlotNumber() * this.schema.getEntrySize();
    byte[] encoded_record = cur_page.readBytes(offset, this.schema.getEntrySize());
    return this.schema.decode(encoded_record);
  }

  /**
   * Update an existing record with new values. Make sure to update this.stats
   * as necessary.
   *
   * @param values the new values of the record
   * @param rid the RecordID of the record to update
   * @return the old version of the record
   * @throws DatabaseException if rid does not correspond to a valid record or
   *         if the values do not correspond to the schema of this table
   */
  public Record updateRecord(List<DataType> values, RecordID rid) throws DatabaseException {
    if (!this.checkRecordIDValidity(rid)) {
      throw new DatabaseException("rid does not correspond to a valid record");
    }
    Record old_record = this.getRecord(rid);
    try {
      Page cur_page = this.allocator.fetchPage(rid.getPageNum());
      int slot_number = rid.getSlotNumber();
      int offset = this.pageHeaderSize + slot_number * this.schema.getEntrySize();
      Record record_value = this.schema.verify(values);
      byte[] encoded_record = this.schema.encode(record_value);
      cur_page.writeBytes(offset, encoded_record.length, encoded_record);
    } catch (SchemaException e) {
      throw new DatabaseException("values don't match schema");
    }
    return old_record;
  }

  public int getNumEntriesPerPage() {
    return this.numEntriesPerPage;
  }

  public int getNumDataPages() {
    return this.allocator.getNumPages() - 1;
  }

  public long getNumRecords() {
    return this.numRecords;
  }

  public Schema getSchema() {
    return this.schema;
  }

  public TableStats getStats() { return this.stats; }

  /**
   * Check whether a RecordID is valid or not. That is, check to see if the slot
   * in the page specified by the RecordID contains a valid record (i.e. whether
   * the bit in the slot bitmap is set to 1).
   *
   * @param rid the record id to check
   * @return true if rid corresponds to a valid record, otherwise false
   * @throws DatabaseException if rid does not reference an existing data page slot
   */
  private boolean checkRecordIDValidity(RecordID rid) throws DatabaseException {
    if (rid.getSlotNumber() > numEntriesPerPage - 1) {
      return false;
    }
    try {
      int page_num = rid.getPageNum();
      int slot_num = rid.getSlotNumber();
      Page cur_page = this.allocator.fetchPage(page_num);
      byte[] header = this.readPageHeader(cur_page);
      int byte_num = slot_num / 8;
      byte block = header[byte_num];
      int bitOffset = 7 - (slot_num % 8);
      byte byte_slot = (byte) (0x01 << bitOffset);
      if ((block & byte_slot) == byte_slot) {
        return true;
      }
      return false;
    } catch (PageException e) {
      throw new DatabaseException("rid does not reference an existing data page slot");
    }
  }

  /**
   * Based on the Schema known to this table, calculate the number of record
   * entries a data page can hold and the size (in bytes) of the page header.
   * The page header only contains the slot bitmap and takes up no other space.
   * For ease of calculations and to prevent header byte splitting, ensure that
   * `numEntriesPerPage` is a multiple of 8 (this may waste some space).
   *
   * Should set this.pageHeaderSize and this.numEntriesPerPage.
   */
  private void setEntryCounts() {
    int eight_entries = this.schema.getEntrySize() * 8 + 1;
    int eigth_num_entries = Page.pageSize / eight_entries;
    this.pageHeaderSize = eigth_num_entries;
    this.numEntriesPerPage = eigth_num_entries * 8;
  }

  /**
   * Checks if there is any free space on the given page.
   *
   * @param p the page to check
   * @return true if there exists free space, otherwise false
   */
  private boolean spaceOnPage(Page p) {
    byte[] header = this.readPageHeader(p);

    for (byte b : header) {
      if (b != (byte) 0xFF) {
        return true;
      }
    }

    return false;
  }

  /**
   * Checks how many valid record entries are in the given page.
   *
   * @param p the page to check
   * @return number of record entries in p
   */
  private int numValidEntries(Page p) {
    byte[] header = this.readPageHeader(p);
    int count = 0;

    for (byte b : header) {
      for (int mask = 0x01; mask != 0x100; mask <<= 1) {
        if ((b & (byte) mask) != 0) {
          count++;
        }
      }
    }

    return count;
  }

  /**
   * Utility method to write the header page of the table. The only information written into
   * the header page is the table's schema.
   */
  private void writeHeaderPage() {
    int numBytesWritten = 0;
    Page headerPage = this.allocator.fetchPage(this.allocator.allocPage());

    assert(0 == headerPage.getPageNum());

    List<String> fieldNames = this.schema.getFieldNames();
    headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(fieldNames.size()).array());
    numBytesWritten += 4;

    for (String fieldName : fieldNames) {
      headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(fieldName.length()).array());
      numBytesWritten += 4;
    }

    for (String fieldName : fieldNames) {
      headerPage.writeBytes(numBytesWritten, fieldName.length(), fieldName.getBytes(Charset.forName("UTF-8")));
      numBytesWritten += fieldName.length();
    }

    for (DataType field : this.schema.getFieldTypes()) {
      headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(field.type().ordinal()).array());
      numBytesWritten += 4;

      if (field.type().equals(DataType.Types.STRING)) {
        headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(field.getSize()).array());
        numBytesWritten += 4;
      }
    }
  }

  /**
   * Utility method to read the header page of the table.
   */
  private void readHeaderPage() {
    int numBytesRead = 0;
    Page headerPage = this.allocator.fetchPage(0);

    int numFields = ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt();
    numBytesRead += 4;

    List<Integer> fieldNameLengths = new ArrayList<Integer>();
    for (int i = 0; i < numFields; i++) {
      fieldNameLengths.add(ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt());
      numBytesRead += 4;
    }

    List<String> fieldNames = new ArrayList<String>();
    for (int fieldNameLength : fieldNameLengths) {
      byte[] bytes = headerPage.readBytes(numBytesRead, fieldNameLength);

      fieldNames.add(new String(bytes, Charset.forName("UTF-8")));
      numBytesRead += fieldNameLength;
    }

    List<DataType> fieldTypes = new ArrayList<DataType>();
    for (int i = 0; i < numFields; i++) {
      int ordinal = ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt();
      DataType.Types type = DataType.Types.values()[ordinal];
      numBytesRead += 4;

      switch(type) {
        case INT:
          fieldTypes.add(new IntDataType());
          break;
        case STRING:
          int len = ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt();
          numBytesRead += 4;

          fieldTypes.add(new StringDataType(len));
          break;
        case BOOL:
          fieldTypes.add(new BoolDataType());
          break;
        case FLOAT:
          fieldTypes.add(new FloatDataType());
          break;
      }
    }

    this.schema = new Schema(fieldNames, fieldTypes);

  }

  /**
   * Utility method to write a particular bit into the header of a particular page.
   *
   * @param page the page to modify
   * @param slotNum the header slot to modify
   * @param value the value of the bit to write (should either be 0 or 1)
   */
  private void writeBitToHeader(Page page, int slotNum, byte value) {
    byte[] header = this.readPageHeader(page);
    int byteOffset = slotNum / 8;
    int bitOffset = 7 - (slotNum % 8);

    if (value == 0) {
      byte mask = (byte) ~((1 << bitOffset));

      header[byteOffset] = (byte) (header[byteOffset] & mask);
      page.writeBytes(0, this.pageHeaderSize, header);
    } else {
      byte mask = (byte) (1 << bitOffset);

      header[byteOffset] = (byte) (header[byteOffset] | mask);
    }

    page.writeBytes(0, this.pageHeaderSize, header);
  }

  /**
   * Read the slot header of a page.
   *
   * @param page the page to read from
   * @return a byte[] with the slot header
   */
  public byte[] readPageHeader(Page page) {
    return page.readBytes(0, this.pageHeaderSize);
  }

  public int getPageHeaderSize() {
    return this.pageHeaderSize;
  }

  public int getEntrySize()  {
    return this.schema.getEntrySize();
  }

  public int getNumPages() { return this.allocator.getNumPages(); }

  /**
   * An implementation of Iterator that provides an iterator interface over all
   * of the records in this table.
   */
  private class TableIterator implements Iterator<Record> {
    private Iterator<Page> p_iter;
    private Page cur_page;
    private byte[] cur_page_header;
    private int next_slot;

    public TableIterator() {
      this.p_iter = Table.this.allocator.iterator();
      p_iter.next();
      if (p_iter.hasNext()) {
        this.cur_page = p_iter.next();
        this.cur_page_header = Table.this.readPageHeader(this.cur_page);
      }
      this.next_slot = 0;
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      if (cur_page == null) {
        return false;
      }
      if (this.getNextFilledSlotNum(this.next_slot, cur_page_header) >= 0) {
        return true;
      }
      else {
        while(this.p_iter.hasNext()) {
          this.cur_page = p_iter.next();
          this.cur_page_header = Table.this.readPageHeader(this.cur_page);
          this.next_slot = 0;
          if (this.getNextFilledSlotNum(this.next_slot, cur_page_header) >= 0) {
            return true;
          }
        }
      }
      return false;
    }

    /**
     *
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
      if (!this.hasNext()) {
        throw new NoSuchElementException("no more records to yield");
      }
      int page_num = this.cur_page.getPageNum();
      int slot_num = this.getNextFilledSlotNum(this.next_slot, this.cur_page_header);
      this.next_slot = slot_num + 1;
      try {
        return Table.this.getRecord(new RecordID(page_num, slot_num));
      } catch (DatabaseException e) {
        System.out.println("Database Exception when trying to get Record");
      }
      return null;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
