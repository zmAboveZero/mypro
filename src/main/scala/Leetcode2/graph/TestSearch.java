package Leetcode2.graph;

public class TestSearch {
    public static void main(String[] args) {
        int n = 8, e = 9;
        String[] labels = {"1", "2", "3", "4", "5", "6", "7", "8"};
        AMWGraph graph = new AMWGraph(n);
        for (String label : labels) {
            graph.insertVertex(label);
        }
        graph.insertEdge(0, 1, 1);
        graph.insertEdge(0, 2, 1);
        graph.insertEdge(1, 3, 1);
        graph.insertEdge(1, 4, 1);
        graph.insertEdge(3, 7, 1);
        graph.insertEdge(4, 7, 1);
        graph.insertEdge(2, 5, 1);
        graph.insertEdge(2, 6, 1);
        graph.insertEdge(5, 6, 1);
        graph.insertEdge(1, 0, 1);
        graph.insertEdge(2, 0, 1);
        graph.insertEdge(3, 1, 1);
        graph.insertEdge(4, 1, 1);
        graph.insertEdge(7, 3, 1);
        graph.insertEdge(7, 4, 1);
        graph.insertEdge(6, 2, 1);
        graph.insertEdge(5, 2, 1);
        graph.insertEdge(6, 5, 1);
        System.out.println("DFS");

//        graph.depthFirstSearch();
        graph.broadFirstSearch();

    }
}
