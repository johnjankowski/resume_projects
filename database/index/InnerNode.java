package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;

import java.util.List;

/**
 * A B+ tree inner node. An inner node header contains the page number of the
 * parent node (or -1 if no parent exists), and the page number of the first
 * child node (or -1 if no child exists). An inner node contains InnerEntry's.
 * Note that an inner node can have duplicate keys if a key spans multiple leaf
 * pages.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class InnerNode extends BPlusNode {

  public InnerNode(BPlusTree tree) {
    super(tree, false);
    getPage().writeByte(0, (byte) 0);
    setFirstChild(-1);
    setParent(-1);
  }
  
  public InnerNode(BPlusTree tree, int pageNum) {
    super(tree, pageNum, false);
    if (getPage().readByte(0) != (byte) 0) {
      throw new BPlusTreeException("Page is not Inner Node!");
    }
  }

  @Override
  public boolean isLeaf() {
    return false;
  }

  public int getFirstChild() {
    return getPage().readInt(5);
  }
  
  public void setFirstChild(int val) {
    getPage().writeInt(5, val);
  }

  /**
   * See BPlusNode#locateLeaf documentation.
   */
  @Override
  public LeafNode locateLeaf(DataType key, boolean findFirst) {
    if (key == null) {
      return null;
    }
    List<BEntry> all_entries = this.getAllValidEntries();
    int i = 0;
    int prev = getFirstChild();
    while (i < all_entries.size()) {
      BEntry curr = all_entries.get(i);
      if (key.compareTo(curr.getKey()) < 0) {
        return getBPlusNode(getTree(), prev).locateLeaf(key, findFirst);
      }
      i++;
      prev = curr.getPageNum();
    }
    return getBPlusNode(getTree(), all_entries.get(i - 1).getPageNum()).locateLeaf(key, findFirst);
  }

  /**
   * Splits this node and pushes up the middle key. Note that we split this node
   * immediately after it becomes full rather than when trying to insert an
   * entry into a full node. Thus a full inner node of 2d entries will be split
   * into a left node with d entries and a right node with d-1 entries, with the
   * middle key pushed up.
   */
  @Override
  public void splitNode() {
    List<BEntry> all_entries = this.getAllValidEntries();
    List<BEntry> left_list = all_entries.subList(0, all_entries.size() / 2);
    List<BEntry> right_list = all_entries.subList((all_entries.size() / 2) + 1, all_entries.size());
    InnerNode parent_node;
    if (getParent() >= 0) {
      parent_node = (InnerNode) getBPlusNode(getTree(), getParent());
    }
    else {
      parent_node = new InnerNode(getTree());
      parent_node.setFirstChild(this.getPageNum()); //set first child of parent node to this node
      this.setParent(parent_node.getPageNum()); // set leaf's parent to be this node
      getTree().updateRoot(parent_node.getPageNum()); //update root with new root
    }
    overwriteBNodeEntries(left_list); // overwrite vals of left with d smallest
    InnerNode right = new InnerNode(getTree()); //create new right
    right.overwriteBNodeEntries(right_list); // put d-1 largest in right
    //set child of right to child of pushed entry
    right.setFirstChild(all_entries.get(all_entries.size() / 2).getPageNum());
    right.setParent(parent_node.getPageNum()); //set parent of right to parent node
    //make new innerentry with key of pushed entry and pagenum goes to right
    BEntry right_parent_entry = new InnerEntry(all_entries.get(all_entries.size() / 2).getKey(), right.getPageNum());
    parent_node.insertBEntry(right_parent_entry); //insert new entry in parent
    return;
  }
}
