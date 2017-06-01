package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * A B+ tree leaf node. A leaf node header contains the page number of the
 * parent node (or -1 if no parent exists), the page number of the previous leaf
 * node (or -1 if no previous leaf exists), and the page number of the next leaf
 * node (or -1 if no next leaf exists). A leaf node contains LeafEntry's.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class LeafNode extends BPlusNode {

  public LeafNode(BPlusTree tree) {
    super(tree, true);
    getPage().writeByte(0, (byte) 1);
    setPrevLeaf(-1);
    setParent(-1);
    setNextLeaf(-1);
  }
  
  public LeafNode(BPlusTree tree, int pageNum) {
    super(tree, pageNum, true);
    if (getPage().readByte(0) != (byte) 1) {
      throw new BPlusTreeException("Page is not Leaf Node!");
    }
  }

  @Override
  public boolean isLeaf() {
    return true;
  }

  /**
   * See BPlusNode#locateLeaf documentation.
   */
  @Override
  public LeafNode locateLeaf(DataType key, boolean findFirst) {
    if (key == null) {
      return null;
    }
    if (findFirst) {
      if (getPrevLeaf() >= 0) {
        LeafNode prev = (LeafNode) getBPlusNode(getTree(), getPrevLeaf());
        List<BEntry> prev_vals = prev.getAllValidEntries();
        if (key.compareTo(prev_vals.get(prev_vals.size() - 1).getKey()) > 0 ) {
          return this;
        }
        return prev.locateLeaf(key, findFirst);
      }
    }
    else {
      if (getNextLeaf() >= 0) {
        LeafNode next = (LeafNode) getBPlusNode(getTree(), getNextLeaf());
        List<BEntry> next_vals = next.getAllValidEntries();
        if (key.compareTo(next_vals.get(0).getKey()) < 0) {
          return this;
        }
        return next.locateLeaf(key, findFirst);
      }
    }
    return this;
  }

  /**
   * Splits this node and copies up the middle key. Note that we split this node
   * immediately after it becomes full rather than when trying to insert an
   * entry into a full node. Thus a full leaf node of 2d entries will be split
   * into a left node with d entries and a right node with d entries, with the
   * leftmost key of the right node copied up.
   */
  @Override
  public void splitNode() {
    InnerNode parent_node;
    if (getParent() >= 0) {
      parent_node = (InnerNode) getBPlusNode(getTree(), getParent()); //already has a parent so fetch it
    }
    else {
      parent_node = new InnerNode(getTree()); //no parent so create one
      parent_node.setFirstChild(this.getPageNum()); //set its child to current leaf node
      this.setParent(parent_node.getPageNum()); // set leaf's parent to be this node
      getTree().updateRoot(parent_node.getPageNum()); //update root with new root
    }
    List<BEntry> all_entries = this.getAllValidEntries(); //get all entries
    List<BEntry> left_list = all_entries.subList(0, all_entries.size() / 2); //make sublist of d smallest entries
    List<BEntry> right_list = all_entries.subList((all_entries.size() / 2), all_entries.size()); //sublist of d biggest
    LeafNode right = new LeafNode(getTree()); //make new leafnode to copy bigger entries to
    right.overwriteBNodeEntries(right_list); // copy bigger entries to right node
    right.setParent(parent_node.getPageNum()); // right's parent set to parent node
    right.setNextLeaf(this.getNextLeaf()); // right's next set to next node
    right.setPrevLeaf(this.getPageNum()); // right's prev set to left
    if (this.getNextLeaf() >= 0) { // only do this if there is actually a next node
      LeafNode next_right = (LeafNode) getBPlusNode(getTree(), this.getNextLeaf()); // get next node
      next_right.setPrevLeaf(right.getPageNum()); //sets the prev of next node to right
    }
    overwriteBNodeEntries(left_list);
    this.setNextLeaf(right.getPageNum()); //left node next set to right
    BEntry right_parent_entry = new InnerEntry(all_entries.get(all_entries.size() / 2).getKey(), right.getPageNum());
    parent_node.insertBEntry(right_parent_entry);
    return;
  }
  
  public int getPrevLeaf() {
    return getPage().readInt(5);
  }

  public int getNextLeaf() {
    return getPage().readInt(9);
  }
  
  public void setPrevLeaf(int val) {
    getPage().writeInt(5, val);
  }

  public void setNextLeaf(int val) {
    getPage().writeInt(9, val);
  }

  /**
   * Creates an iterator of RecordID's for all entries in this node.
   *
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scan() {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      rids.add(le.getRecordID());
    }

    return rids.iterator();
  }

  /**
   * Creates an iterator of RecordID's whose keys are greater than or equal to
   * the given start value key.
   *
   * @param startValue the start value key
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scanFrom(DataType startValue) {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      if (startValue.compareTo(le.getKey()) < 1) { 
        rids.add(le.getRecordID());
      }
    }
    return rids.iterator();
  }

  /**
   * Creates an iterator of RecordID's that correspond to the given key.
   *
   * @param key the search key
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scanForKey(DataType key) {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      if (key.compareTo(le.getKey()) == 0) { 
        rids.add(le.getRecordID());
      }
    }
    return rids.iterator();
  }
}
