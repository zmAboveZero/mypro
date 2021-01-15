package Leetcode2.graph;

import java.util.ArrayList;
import java.util.LinkedList;

public class AMWGraph {
    private ArrayList vertexList;
    private int[][] edges;
    private int numOfEdges;
    private boolean[] isVisited = {false, false, false, false, false, false, false, false};

    public AMWGraph(int n) {
        edges = new int[n][n];
        vertexList = new ArrayList(n);
        numOfEdges = 0;
    }


    public int getnumOfVertex() {
        return vertexList.size();
    }

    public int getNumOfEdges() {
        return numOfEdges;
    }

    public Object getValueByIndex(int i) {
        return vertexList.get(i);
    }

    public int getWeight(int v1, int v2) {
        return edges[v1][v2];
    }

    public void insertVertex(Object vertex) {
        vertexList.add(vertexList.size(), vertex);
    }

    public void insertEdge(int v1, int v2, int weight) {
        edges[v1][v2] = weight;
        numOfEdges++;
    }

    public void deleteEdge(int v1, int v2) {
        edges[v1][v2] = 0;
        numOfEdges--;
    }


    public int getFirstNeighbor(int index) {
        for (int j = 0; j < vertexList.size(); j++) {
            if (edges[index][j] > 0) {
                return j;
            }
        }
        return -1;
    }

    public int getNextNeighbor(int v1, int v2) {
        for (int j = v2 + 1; j < vertexList.size(); j++) {
            if (edges[v1][j] > 0) {
                return j;
            }
        }
        return -1;
    }


    private void depthFirstSearch(boolean[] isVisited, int i) {
        System.out.print(getValueByIndex(i) + " ");
        isVisited[i] = true;
        int w = getFirstNeighbor(i);
        while (w != -1) {
            if (!isVisited[w]) {
                depthFirstSearch(isVisited, w);
            }
            w = getNextNeighbor(i, w);
        }
    }


    public void depthFirstSearch() {
        for (int i = 0; i < getnumOfVertex(); i++) {
            if (!isVisited[i]) {
                depthFirstSearch(isVisited, i);
            }
        }
    }


    public void broadFirstSearch() {
        for (int i = 0; i < getnumOfVertex(); i++) {
            if (!isVisited[i]) {
                broadFirstSearch(isVisited, i);
            }
        }
    }

    private void broadFirstSearch(boolean[] isVisited, int i) {
        int u, w;
        LinkedList queue = new LinkedList();
        System.out.println(getValueByIndex(i) + " ");
        isVisited[i] = true;
        queue.addLast(i);
        while (!queue.isEmpty()) {
            u = ((Integer) queue.removeFirst()).intValue();
            w = getFirstNeighbor(u);
            while (w != -1) {
                if (!isVisited[w]) {
                    System.out.println(getValueByIndex(w) + " ");
                    isVisited[w] = true;
                    queue.addLast(w);
                }
                w = getNextNeighbor(u, w);
            }
        }
    }
}
