import java.util.*;
import java.io.*;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.Date;


public class CommitTree implements Serializable {

	public int commitNumber;
	public CommitNode currentBranchHead;
	public HashMap<String, CommitNode> branches;
	public HashMap<String, File> files;
	public String branch;
	private HashMap<String, ArrayList<Integer>> messageID;
	public HashMap<Integer, CommitNode> commitByID;
	
	public CommitTree() {
		commitNumber = 0;
		CommitNode first = new CommitNode("initial commit");
		currentBranchHead = first;
		branches = new HashMap<String, CommitNode>();
		branches.put("master", currentBranchHead);
		files = new HashMap<String, File>();
		branch = "master";
		messageID = new HashMap<String, ArrayList<Integer>>();
		ArrayList<Integer> thisMessage = new ArrayList<Integer>();
		thisMessage.add(currentBranchHead.commitID);
		messageID.put(currentBranchHead.message, thisMessage);
		commitByID = new HashMap<Integer, CommitNode>();
		commitByID.put(0, currentBranchHead);
	}

	public void add(String mes, Map<String, File> newFiles) {
		commitNumber += 1;
		CommitNode c = new CommitNode(mes, currentBranchHead, newFiles, commitNumber);
		currentBranchHead = c;
		commitByID.put(commitNumber, currentBranchHead);
		branches.put(branch, currentBranchHead);
		if (messageID.containsKey(currentBranchHead.message)) {
			messageID.get(currentBranchHead.message).add(currentBranchHead.commitID);
		}
		if (!messageID.containsKey(currentBranchHead.message)) {
			ArrayList<Integer> thisMessage = new ArrayList<Integer>();
			thisMessage.add(currentBranchHead.commitID);
			messageID.put(currentBranchHead.message, thisMessage);
		}
	}

	private void addRebase(CommitNode c, CommitNode prev, CommitNode split) {
		commitNumber += 1;
		CommitNode next = new CommitNode(c, prev, split, commitNumber);
		commitByID.put(commitNumber, next);
		messageID.get(next.message).add(next.commitID);
	}

	public void changeBranchHead(CommitNode c) {
		branches.put(branch, c);
		currentBranchHead = c;
	}

	public boolean currentInGiven(String b) {
		CommitNode c = currentBranchHead;
		CommitNode g = branches.get(b);
		while (g != null) {
			if (g == c) {
				return true;
			}
			g = g.prev;
		}
		return false;
	}

	public boolean inHistory(String b) {
		CommitNode c = currentBranchHead;
		CommitNode p = branches.get(b);
		while (c != null) {
			if (c == p) {
				return true;
			}
			c = c.prev;
		}
		return false;
	}

	private int countDistance(CommitNode c) {
		int i = 0;
		CommitNode d = currentBranchHead;
		while (d != c) {
			i += 1;
			d = d.prev;
		}
		return i;
	}

	private CommitNode getNode(int i) {
		CommitNode c = currentBranchHead;
		while (i > 1) {
			c = c.prev;
		}
		return c;
	}

	private void interactive(CommitNode current, CommitNode prev, CommitNode split, String b) {
		System.out.println("Currently replaying:");
		commitInfo(current);
		System.out.println("Would you like to (c)ontinue, (s)kip this commit, or change this commit's (m)essage?");
		Scanner in = new Scanner(System.in);
		String command = in.nextLine();
		if (command.equals("c")) {
			addRebase(current, prev, split);
			in.close();
			return;
		}
		if (command.equals("s")) {
			if (current == currentBranchHead || prev == branches.get(b)) {
				System.out.println("Cannot skip initial or final commit.");
				in.close();
				interactive(current, prev, split, b);
				return;
			}
			in.close();
			return;
		}
		if (command.equals("m")) {
			System.out.println("Please enter a new message for this commit.");
			String newMessage = in.nextLine();
			current.message = newMessage;
			in.close();
			return;
		}
	}

	public void interactiveRebase(String b) {
		CommitNode c = branches.get(b);
		CommitNode split = getSplitNode(b);
		int counter = countDistance(split);
		while (counter > 1) {
			CommitNode next = getNode(counter);
			interactive(next, c, split, b);
			c = next;
			counter -= 1;
		}
		CommitNode newHead = getNode(counter);
		interactive(newHead, c, split, b);
		branches.put(branch, newHead);
		currentBranchHead = newHead;
	}

	public void rebase(String b) {
		CommitNode c = branches.get(b);
		CommitNode split = getSplitNode(b);
		int counter = countDistance(split);
		while (counter > 1) {
			CommitNode next = getNode(counter);
			addRebase(next, c, split);
			c = next;
			counter -= 1;
		}
		CommitNode newHead = getNode(counter);
		addRebase(newHead, c, split);
		branches.put(branch, newHead);
		currentBranchHead = newHead;
	}

	private CommitNode getSplitNode(String b) {
		CommitNode c = currentBranchHead;
		CommitNode p = branches.get(b);
		while (p!= null) {
			while (c != null) {
				if (c == p) {
					return c;
				}
				c = c.prev;
			}
			c = currentBranchHead;
			p = p.prev;
		}
		return null;
	}

	public List<String> getSplitFileIDs(String b) {
		CommitNode c = currentBranchHead;
		CommitNode p = branches.get(b);
		while (p!= null) {
			while (c != null) {
				if (c == p) {
					return c.commitFiles;
				}
				c = c.prev;
			}
			c = currentBranchHead;
			p = p.prev;
		}
		return null;
	}

	public List<String> getSplitFiles(String b) {
		CommitNode c = currentBranchHead;
		CommitNode p = branches.get(b);
		while (c != null && p != null) {
			if (c == p) {
				return c.commitFileNames;
			}
			c = c.prev;
			p = p.prev;
		}
		return null;
	}

	public void removeBranch(String b) {
		branches.remove(b);
	}

	public void addBranch(String b) {
		branches.put(b, currentBranchHead);
	}

	public void switchBranch(String b) {
		branch = b;
		currentBranchHead = branches.get(b);
	}

	public void dontInherit(String file) {
		if (!currentBranchHead.commitFileNames.contains(file)) {
			System.out.println("No reason to remove the file.");
			return;
		}
		currentBranchHead.remove.add(file);
	}

	public void dontInheritSilently(String file) {
		if (!currentBranchHead.commitFileNames.contains(file)) {
			return;
		}
		currentBranchHead.remove.add(file);
	}

	public void globalLog() {
		Set<Map.Entry<Integer,CommitNode>> set = commitByID.entrySet();
		for (Map.Entry<Integer,CommitNode> entry : set) {
			System.out.println("====");
			System.out.println("Commit " + Integer.toString(entry.getKey()) + ".");
			System.out.println(entry.getValue().timeStamp);
			System.out.println(entry.getValue().message);
			System.out.println("");
		}
	}

	private void commitInfo(CommitNode c) {
		System.out.println("====");
		System.out.println("Commit " + Integer.toString(c.commitID) + ".");
		System.out.println(c.timeStamp);
		System.out.println(c.message);
		System.out.println("");
	}

	public void log() {
		CommitNode p = currentBranchHead;
		while (p != null) {
			System.out.println("====");
			System.out.println("Commit " + Integer.toString(p.commitID) + ".");
			System.out.println(p.timeStamp);
			System.out.println(p.message);
			System.out.println("");
			p = p.prev;
		}
	}

	public void find(String s) {
		ArrayList<Integer> ids = messageID.get(s);
		for (int i = 0; i < ids.size(); i++) {
			System.out.println(ids.get(i));
		}
	}

	public void printBranches() {
		Set<String> keys = branches.keySet();
		Iterator<String> keyIt = keys.iterator();
		while (keyIt.hasNext()) {
			String line = keyIt.next();
			if (line.equals(branch)) {
				System.out.println("*" + line);
			}
			else {
				System.out.println(line);
			}
		}
	}

	public class CommitNode implements Serializable {

		private CommitNode prev;
		private int commitID;
		public String message;
		private String timeStamp;
		public ArrayList<String> remove;
		//corresponds to the key of the files in this commit in the fileslist
		public ArrayList<String> commitFiles;
		public ArrayList<String> commitFileNames;

		private CommitNode(CommitNode current, CommitNode previous, CommitNode split, int id) {
			remove = new ArrayList<String>();
			message = current.message;
			prev = previous;
			commitID = id;
			Date now = new Date();
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			String commitDate = format.format(now);
			timeStamp = commitDate;
			//file names plus commit id
			commitFiles = new ArrayList<String>();
			//file names
			commitFileNames = new ArrayList<String>();
			//need to see if current has files unmodified since split, but modified in previous
			//-inherit those files
			//then inherit everything else in current
			ArrayList<String> splitFiles = split.commitFileNames;
			ArrayList<String> splitFileIDs = split.commitFiles;
			ArrayList<String> previousFiles = previous.commitFileNames;
			ArrayList<String> previousFileIDs = previous.commitFiles;
			ArrayList<String> currentFiles = current.commitFileNames;
			ArrayList<String> currentFileIDs = current.commitFiles;
			for (int i = 0; i < currentFileIDs.size(); i++) {
				if (splitFileIDs.contains(currentFileIDs.get(i)) && previousFiles.contains(currentFiles.get(i)) &&
					(!previousFileIDs.contains(currentFileIDs.get(i)))) {
					int j = previousFiles.indexOf(currentFiles.get(i));
					commitFiles.add(previousFileIDs.get(j));
					commitFileNames.add(previousFiles.get(j));
				}
				else {
					commitFileNames.add(currentFiles.get(i));
					commitFiles.add(currentFileIDs.get(i));
				}
			}
		}

		private CommitNode(String mes) {
			remove = new ArrayList<String>();
			message = mes;
			prev = null;
			commitID = commitNumber;
			Date now = new Date();
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			String commitDate = format.format(now);
			timeStamp = commitDate;
			//file names plus commit id
			commitFiles = new ArrayList<String>();
			//file names
			commitFileNames = new ArrayList<String>();
		}

		private CommitNode(String mes, CommitNode previous, Map<String, File> newFiles, int id) {
			remove = new ArrayList<String>();
			message = mes;
			prev = previous;
			commitID = id;
			Date now = new Date();
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			String commitDate = format.format(now);
			timeStamp = commitDate;
			commitFiles = new ArrayList<String>();
			//file names
			commitFileNames = new ArrayList<String>();

			/* need to check if filenames are equal, if they are then dont
			add the index to commitFiles, if they arent then add index to commitFiles
			After all old file indeces are stored in commitFiles, add all newFiles
			to files and add their indeces to commitFiles.
			*/
			
			for (int i = 0; i < prev.commitFileNames.size(); i++) {
				if (!newFiles.containsKey(prev.commitFileNames.get(i)) && !prev.remove.contains(prev.commitFileNames.get(i))) {
					commitFiles.add(prev.commitFiles.get(i));
					commitFileNames.add(prev.commitFileNames.get(i));
				}
			}
			HashSet<String> newFileNames = new HashSet<String>();
			newFileNames.addAll(newFiles.keySet());
			for (String newFile : newFileNames) {
				files.put(newFile + Integer.toString(commitID), newFiles.get(newFile));
				commitFiles.add(newFile + Integer.toString(commitID));
				commitFileNames.add(newFile);
			}
		}

	}
}

