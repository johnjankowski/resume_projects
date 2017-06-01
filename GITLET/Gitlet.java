import java.util.*;
import java.io.*;
import java.nio.file.*;

public class Gitlet {

	private static CommitTree gitletTree;

	private static void copyFile(File start, File end) throws IOException {
		Files.copy(start.toPath(), end.toPath());
	}

    private static void restore(File old, File restored) {
        try {
            String pathname = old.getPath();
            if (old.exists()) {
               old.delete();
            }
            File copy = new File(pathname);
            copyFile(restored, copy);
            copy.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	private static boolean areSame(File one, File two) throws IOException{
		BufferedReader checkReader = new BufferedReader(new FileReader(one));
        BufferedReader currentReader = new BufferedReader(new FileReader(two));
        boolean tempFlag = true;
        while (tempFlag) {
        	String checkone = checkReader.readLine();
        	String checktwo = currentReader.readLine();
        	if (checkone == null && checktwo == null) {
        		return true;
        	}
        	if (!checkone.equals(checktwo)) {
        		return false;
        	}
        }
        return true;
	}

	private static void addToRemoveFile(String s) {
		File removeFile = new File(".gitlet/remove.txt");
		try {
			boolean didntexist = removeFile.createNewFile();
			//already there
			if (!didntexist) {
				//checking to see if already added
           		BufferedReader checkReader = new BufferedReader(new FileReader(removeFile));
           		boolean o = true;
           		while (o) {
           			String aline = checkReader.readLine();
           			if (aline == null) {
           				o = false;
           			}
           			if (aline.equals(s)) {
           				return;
           			}
           		}
				//adding to file
				File temp = new File(".gitlet/temp.txt");
            	temp.createNewFile();
           		BufferedReader removeReader = new BufferedReader(new FileReader(removeFile));
       			PrintWriter newWriter = new PrintWriter(temp, "UTF-8");
            	boolean flag = true;
            	while (flag) {
            		String newLine = removeReader.readLine();
            		if (newLine == null) {
            			flag = false;
            			newWriter.close();
            		}
            		if (flag) {	
            			newWriter.println(newLine);
            		}
            	}
            	PrintWriter removeWriter = new PrintWriter(removeFile, "UTF-8");
            	BufferedReader tempReader = new BufferedReader(new FileReader(temp));
            	flag = true;
            	while (flag) {
            		String newLine = tempReader.readLine();
            		if (newLine == null) {
            			flag = false;
            			removeWriter.println(s);
            			removeWriter.close();
            		}
            		if (flag) {
            			removeWriter.println(newLine);
            		}
            	}
            	temp.delete();
			}
			//just made
			if (didntexist) {
				PrintWriter writer = new PrintWriter(removeFile, "UTF-8");
				writer.println(s);
				writer.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void printTextFile(File f) {
		if (f.exists()) {
			try {
				BufferedReader textReader = new BufferedReader(new FileReader(f));
				while (true) {
					String line = textReader.readLine();
					if (line == null) {
						return;
					}
					System.out.println(line);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		boolean initFlag = false;
		gitletTree = tryLoadingCommitTree();
		switch (args[0]) {
            case "init":
            	File gitlet = new File(".gitlet");
         		initFlag = gitlet.mkdir();
         		if (!initFlag) {
         			System.out.println("A gitlet version control system already exists in the current directory.");
         			break;
         		}
         		gitletTree = new CommitTree();
         		saveMyCommitTree(gitletTree);
                break;
            /* want to create a file with filenames
            check to see if add is already a file.
            --	if it is not then create a file add.txt
            	then write the filename to it
			--  if it is then add the filename to the existing file
				unless it is already there
			JUST NEED TO CHECK IF ITS ALREADY BEEN ADDED TO ADD.txt
            */
            case "add":
            	if (args.length < 2) {
            		System.out.println("Type in a file name.");
            		break;
            	}
            	String fileName = args[1];
            	if (!new File(fileName).exists()) {
            		System.out.println("File does not exist.");
            		break;
            	}
            	//checking if it has changed
            	if (gitletTree.currentBranchHead.commitFileNames.contains(fileName)) {
            		ListIterator<String> commitIterator = gitletTree.currentBranchHead.commitFiles.listIterator();
            		String thisCommit = "";
            		while (commitIterator.hasNext()) {
            			String next = commitIterator.next();
            			if (next.contains(fileName)) {
            				thisCommit = next;
            			}
            		}
            		File checkThis = gitletTree.files.get(thisCommit);
            		File thisFile = new File(fileName);
            		try {
            			if (areSame(checkThis, thisFile) && !gitletTree.currentBranchHead.remove.contains(fileName)) {
            				System.out.println("File has not been modified since the last commit.");
            				break;
            			}
            		} catch (IOException e) {
            			e.printStackTrace();
            		}
            	}
            	//carrying on
            	File add = new File(".gitlet/add.txt");
      
            	try {
            		boolean exists = add.createNewFile();
            		BufferedReader sameReader = new BufferedReader(new FileReader(add));
            		boolean o = true;
            		boolean stop = false;
            		while (o) {
            			String line = sameReader.readLine();
            			if (line == null) {
            				o = false;
            			}
            			if (o && fileName.equals(line)) {
            				System.out.println("File already added.");
            				stop = true;
            				break;
            			}
            		}
            		if (stop) {
            			break;
            		}
            		//if there is not a file
            		if (exists) {
            			PrintWriter writer = new PrintWriter(add, "UTF-8");
            			writer.println(fileName);
            			writer.close();
            		}
            		//if there already is a file
            		if (!exists) {
            			File temp = new File(".gitlet/temp.txt");
            			temp.createNewFile();
            			BufferedReader addReader = new BufferedReader(new FileReader(add));
            			PrintWriter newWriter = new PrintWriter(temp, "UTF-8");
            			boolean flag = true;
            			while (flag) {
            				String newLine = addReader.readLine();
            				if (newLine == null) {
            					flag = false;
            					newWriter.close();
            				}
            				if (flag) {	
            					newWriter.println(newLine);
            				}
            			}
            			PrintWriter addWriter = new PrintWriter(add, "UTF-8");
            			BufferedReader tempReader = new BufferedReader(new FileReader(temp));
            			flag = true;
            			while (flag) {
            				String newLine = tempReader.readLine();
            				if (newLine == null) {
            					flag = false;
            					addWriter.println(fileName);
            					addWriter.close();
            				}
            				if (flag) {
            					addWriter.println(newLine);
            				}
            			}
            			temp.delete();
            		}
            	} catch (IOException e) {
            		e.printStackTrace();
            	}
            	break;
           	//want to get filename from add and then the corresponding file
            	//need to handle removals
            case "commit":
            	String commitID = Integer.toString(gitletTree.commitNumber);
            	String message = args[1];
            	HashMap<String, File> stagedFiles = new HashMap<String, File>();
            	File commits = new File(".gitlet/add.txt");
            	File commitDirectory = new File(".gitlet/" + commitID);
            	commitDirectory.mkdir();
            	if (commits.exists()) {
            		try {
            			BufferedReader addReader = new BufferedReader(new FileReader(commits));
            			boolean flag = true;
            			while (flag) {
            				String commitFileName = addReader.readLine();
            				if (commitFileName == null) {
            					flag = false;
            				}
            				if (flag) {
            					File oldFile = new File(commitFileName);
            					String[] commitFilePathName = commitFileName.split("/");
            					StringBuilder builder = new StringBuilder();
            					for (String s : commitFilePathName) {
            						builder.append(s);
            					}
            					String newCommitFileName = builder.toString();
            					File newFile = new File(".gitlet/" + commitID + "/" + newCommitFileName);
            					copyFile(oldFile, newFile);
            					stagedFiles.put(commitFileName, newFile);
            				}
            			}
            		} catch (IOException e) {
            			e.printStackTrace();
            		}
            	commits.delete();
            	File removals = new File(".gitlet/remove.txt");
            	if (removals.exists()) {
            		removals.delete();
            	}
            	}
            	gitletTree.add(message, stagedFiles);
            	saveMyCommitTree(gitletTree);
            	break;
            case "rm":
            	if (args.length < 2) {
            		System.out.println("Type in a filename.");
            		break;
            	}
            	String afileName = args[1];
            	File a = new File(".gitlet/add.txt");
            	try {
            		addToRemoveFile(afileName);
            		boolean exists = a.createNewFile();

            		//no add file created so need to not inherit
            		if (exists) {
            			gitletTree.dontInherit(afileName);
            		}
            		//there is an add file so need to remove filename from add if it is there
            		//if its not there then do whatya do for no add file
            		if (!exists) {
            			BufferedReader addReader = new BufferedReader(new FileReader(a));
            			boolean p = true;
            			File temp = new File(".gitlet/temp.txt");
            			temp.createNewFile();
            			PrintWriter writer = new PrintWriter(temp, "UTF-8");
            			while (p) {
            				String line = addReader.readLine();
            				if (line == null) {
            					p = false;
            					writer.close();
            				}
            				if (p && !line.equals(afileName)) {
            					writer.println(line);
            				}
            			}
            			boolean notSoSilent = areSame(a, temp);
            			a.delete();
            			File newAdd = new File(".gitlet/add.txt");
            			newAdd.createNewFile();
            			BufferedReader tempReader = new BufferedReader(new FileReader(temp));
            			PrintWriter addWriter = new PrintWriter(newAdd, "UTF-8");
            			p = true;
            			while (p) {
            				String line = tempReader.readLine();
            				if (line == null) {
            					p = false;
            					addWriter.close();
            				}
            				if (p) {
            					addWriter.println(line);
            				}
            			}
            			temp.delete();
            			//removed from add.txt
            			if (notSoSilent) {
            				gitletTree.dontInherit(afileName);
            			}
            			gitletTree.dontInheritSilently(afileName);
            		}
            	} catch (IOException e) {
            		e.printStackTrace();
            	}
            	break;
            case "log":
            	gitletTree.log();
            	break;
            case "global-log":
                gitletTree.globalLog();
            	break;
            case "find":
            	if (args.length < 2) {
            		System.out.println("Enter a message.");
            		break;
            	}
            	String m = args[1];
            	gitletTree.find(m);
            	break;
            case "status":
            	System.out.println("=== Branches ===");
            	gitletTree.printBranches();
            	System.out.println("");
            	System.out.println("=== Staged Files ===");
            	if (new File(".gitlet/add.txt").exists()) {
            		printTextFile(new File(".gitlet/add.txt"));
            	}
            	System.out.println("");
            	System.out.println("=== Files Marked for Removal ===");
            	if (new File(".gitlet/remove.txt").exists()) {
            		printTextFile(new File(".gitlet/remove.txt"));
            	}
            	break;
            case "checkout":
                if (args.length < 2) {
                    System.out.println("Enter a command.");
                    break;
                }
                //if a branch is entered
                if (gitletTree.branches.containsKey(args[1])) {
                    if (gitletTree.branch.equals(args[1])) {
                        System.out.println("No need to checkout the current branch.");
                        break;
                    }
                    gitletTree.switchBranch(args[1]);
                    for (int i = 0; i < gitletTree.currentBranchHead.commitFiles.size(); i++) {
                        File old = new File(gitletTree.currentBranchHead.commitFileNames.get(i));
                        restore(old, gitletTree.files.get(gitletTree.currentBranchHead.commitFiles.get(i)));
                    }
                    saveMyCommitTree(gitletTree);
                    break;
                }
                //if a file name is entered
                if (gitletTree.currentBranchHead.commitFileNames.contains(args[1])) {
                    File old = new File(args[1]);
                    for (int i = 0; i < gitletTree.currentBranchHead.commitFiles.size(); i++) {
                        String s = gitletTree.currentBranchHead.commitFiles.get(i);
                        if (s.startsWith(args[1])) {
                            restore(old, gitletTree.files.get(s));
                        }
                    }
                    break;
                }
                //if a commit id and file is entered
                else if (args.length == 3) {
                    File old = new File(args[2]);
                    if (gitletTree.commitByID.containsKey(Integer.parseInt(args[1]))) {
                        if (gitletTree.commitByID.get(Integer.parseInt(args[1])).commitFileNames.contains(args[2])) {
                            for (int i = 0; i < gitletTree.commitByID.get(Integer.parseInt(args[1])).commitFiles.size(); i++) {
                                String s = gitletTree.commitByID.get(Integer.parseInt(args[1])).commitFiles.get(i);
                                if (s.startsWith(args[2])) {
                                    restore(old, gitletTree.files.get(s));
                                }
                            }
                            break;
                        }
                        else {
                            System.out.println("File does not exist in that commit.");
                            break;
                        }
                    }
                    else {
                        System.out.println("No commit with that id exists.");
                        break;
                    }
                }
                System.out.println("File does not exist in the most recent commit, or no such branch exists.");
            	break;
            case "branch":
                if (args.length < 2) {
                    System.out.println("Enter a branch name.");
                    break;
                }
                gitletTree.addBranch(args[1]);
                saveMyCommitTree(gitletTree);
            	break;
            case "rm-branch":
                if (args.length < 2) {
                    System.out.println("Enter a branch.");
                    break;
                }
                if (args[1].equals(gitletTree.branch)) {
                    System.out.println("Cannot remove the current branch.");
                    break;
                }
                if (!gitletTree.branches.containsKey(args[1])) {
                    System.out.println("A branch with that name does not exist.");
                    break;
                }
                gitletTree.removeBranch(args[1]);
                saveMyCommitTree(gitletTree);
            	break;
            case "reset":
                if (args.length < 2) {
                    System.out.println("Enter a commit id.");
                    break;
                }
                int id = Integer.parseInt(args[1]);
                if (!gitletTree.commitByID.containsKey(id)) {
                    System.out.println("No commit with that id exists.");
                    break;
                }
                ArrayList<String> filenames = gitletTree.commitByID.get(id).commitFileNames;
                ArrayList<String> filenameids = gitletTree.commitByID.get(id).commitFiles;
                for (int i = 0; i < filenames.size(); i++) {
                    File current = new File(filenames.get(i));
                    restore(current, gitletTree.files.get(filenameids.get(i)));
                }
            	break;
            case "merge":
                if (args.length < 2) {
                    System.out.println("Enter a branch.");
                    break;
                }
                if (!gitletTree.branches.containsKey(args[1])) {
                    System.out.println("A branch with that name does not exist.");
                    break;
                }
                if (gitletTree.branch.equals(args[1])) {
                    System.out.println("Cannot merge a branch with itself.");
                    break;
                }
                List<String> splitFileNameIDs = gitletTree.getSplitFileIDs(args[1]);
                List<String> splitFileNames = gitletTree.getSplitFiles(args[1]);
                ArrayList<String> designatedFileIDs = gitletTree.branches.get(args[1]).commitFiles;
                ArrayList<String> designatedFiles = gitletTree.branches.get(args[1]).commitFileNames;
                ArrayList<String> currentFilesIDs = gitletTree.currentBranchHead.commitFiles;
                ArrayList<String> currentFiles = gitletTree.currentBranchHead.commitFileNames;
                for (int i = 0; i < designatedFiles.size(); i++) {
                    String fileID = designatedFileIDs.get(i);
                    String filename = designatedFiles.get(i);
                    int j = currentFiles.indexOf(filename);
                    File f = new File(filename);
                    if ((!splitFileNameIDs.contains(fileID)) && splitFileNameIDs.contains(currentFilesIDs.get(j))) {
                        restore(f, gitletTree.files.get(fileID));
                    }
                    if ((!splitFileNameIDs.contains(fileID)) && (!splitFileNameIDs.contains(currentFilesIDs.get(j)))) {
                        File conflict = new File(".gitlet/" + filename + ".conflicted");
                        try {
                            conflict.createNewFile();
                            copyFile(gitletTree.files.get(fileID), conflict);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            	break;
            case "rebase":
                if (args.length < 2) {
                    System.out.println("Enter a branch.");
                    break;
                }
                if (args[1].equals(gitletTree.branch)) {
                    System.out.println("Cannot rebase a branch onto itself.");
                    break;
                }
                if (!gitletTree.branches.containsKey(args[1])) {
                    System.out.println("A branch with that name does not exist.");
                    break;
                }
                if (gitletTree.inHistory(args[1])) {
                    System.out.println("Already up-to-date.");
                    break;
                }
                if (gitletTree.currentInGiven(args[1])) {
                    gitletTree.changeBranchHead(gitletTree.branches.get(args[1]));
                    break;
                }
                gitletTree.rebase(args[1]);
                ArrayList<String> rebasefilenames = gitletTree.currentBranchHead.commitFileNames;
                ArrayList<String> rebasefilenameids = gitletTree.currentBranchHead.commitFiles;
                for (int i = 0; i < rebasefilenames.size(); i++) {
                    File current = new File(rebasefilenames.get(i));
                    restore(current, gitletTree.files.get(rebasefilenameids.get(i)));
                }
                saveMyCommitTree(gitletTree);
            	break;
            case "i-rebase":
                if (args.length < 2) {
                    System.out.println("Enter a branch.");
                    break;
                }
                if (args[1].equals(gitletTree.branch)) {
                    System.out.println("Cannot rebase a branch onto itself.");
                    break;
                }
                if (!gitletTree.branches.containsKey(args[1])) {
                    System.out.println("A branch with that name does not exist.");
                    break;
                }
                if (gitletTree.inHistory(args[1])) {
                    System.out.println("Already up-to-date.");
                    break;
                }
                if (gitletTree.currentInGiven(args[1])) {
                    gitletTree.changeBranchHead(gitletTree.branches.get(args[1]));
                    saveMyCommitTree(gitletTree);
                    break;
                }
                gitletTree.interactiveRebase(args[1]);
                ArrayList<String> irebasefilenames = gitletTree.currentBranchHead.commitFileNames;
                ArrayList<String> irebasefilenameids = gitletTree.currentBranchHead.commitFiles;
                for (int i = 0; i < irebasefilenames.size(); i++) {
                    File current = new File(irebasefilenames.get(i));
                    restore(current, gitletTree.files.get(irebasefilenameids.get(i)));
                }
                saveMyCommitTree(gitletTree);
            	break;
			default:
                System.out.println("Invalid command.");  
                break;
		}
	}
//change to committrees
	private static CommitTree tryLoadingCommitTree() {
		CommitTree myCommitTree = null;
        File myCommitTreeFile = new File(".gitlet/myCommitTree.ser");
        if (myCommitTreeFile.exists()) {
            try {
                FileInputStream fileIn = new FileInputStream(myCommitTreeFile);
                ObjectInputStream objectIn = new ObjectInputStream(fileIn);
                myCommitTree = (CommitTree) objectIn.readObject();
            } catch (IOException e) {
                String msg = "IOException while loading myCommitTree.";
                System.out.println(msg);
            } catch (ClassNotFoundException e) {
                String msg = "ClassNotFoundException while loading myCommitTree.";
                System.out.println(msg);
            }
        }
        return myCommitTree;
	}

	private static void saveMyCommitTree(CommitTree c) {
		if (c == null) {
            return;
        }
        try {
            File myCommitTreeFile = new File(".gitlet/myCommitTree.ser");
            FileOutputStream fileOut = new FileOutputStream(myCommitTreeFile);
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
            objectOut.writeObject(c);
        } catch (IOException e) {
            String msg = "IOException while saving myCommitTree.";
            System.out.println(msg);
        }
	}
}
