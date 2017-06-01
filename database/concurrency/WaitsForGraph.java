package edu.berkeley.cs186.database.concurrency;

import java.util.*;

/**
 * A waits for graph for the lock manager (used to detect if
 * deadlock will occur and throw a DeadlockException if it does).
 */
public class WaitsForGraph {

  // We store the directed graph as an adjacency list where each node (transaction) is
  // mapped to a list of the nodes it has an edge to.
  private Map<Long, ArrayList<Long>> graph;

  public WaitsForGraph() {
    graph = new HashMap<Long, ArrayList<Long>>();
  }

  public boolean containsNode(long transNum) {
    return graph.containsKey(transNum);
  }

  protected void addNode(long transNum) {
    if (!graph.containsKey(transNum)) {
      graph.put(transNum, new ArrayList<Long>());
    }
  }

  protected void addEdge(long from, long to) {
    if (!this.edgeExists(from, to)) {
      ArrayList<Long> edges = graph.get(from);
      edges.add(to);
    }
  }

  protected void removeEdge(long from, long to) {
    if (this.edgeExists(from, to)) {
      ArrayList<Long> edges = graph.get(from);
      edges.remove(to);
    }
  }

  protected boolean edgeExists(long from, long to) {
    if (!graph.containsKey(from)) {
      return false;
    }
    ArrayList<Long> edges = graph.get(from);
    return edges.contains(to);
  }

  /**
   * Checks if adding the edge specified by to and from would cause a cycle in this
   * WaitsForGraph. Does not actually modify the graph in any way.
   * @param from the transNum from which the edge points
   * @param to the transNum to which the edge points
   * @return
   */
  protected boolean edgeCausesCycle(long from, long to) {
    //TODO: Implement Me!!
    Map<Long, ArrayList<Long>> checkGraph = new HashMap<Long, ArrayList<Long>>();
    checkGraph.putAll(this.graph);
    if (from == to) {
      return false;
    }
    if (!checkGraph.containsKey(from)) {
      return false;
    }
    ArrayList<Long> edges = checkGraph.get(from);
    if (edges.contains(to)) {
      return false;
    }
    edges.add(to);
    Set<Long> visited = new HashSet<>();
    Set<Long> nodes = checkGraph.keySet();
    for (long node: nodes) {
      if (!visited.contains(node)) {
        HashSet<Long> currVisited = new HashSet<>();
        LinkedList<Long> fringe = new LinkedList<>();
        fringe.add(node);
        while (!fringe.isEmpty()) {
          Long currNode = fringe.pollFirst();
          if (currVisited.contains(currNode)) {
            return true;
          }
          currVisited.add(node);
          visited.add(node);
          if (checkGraph.get(currNode) != null) {
            fringe.addAll(0, checkGraph.get(currNode));
          }
        }
      }
    }

    return false;
  }

}
