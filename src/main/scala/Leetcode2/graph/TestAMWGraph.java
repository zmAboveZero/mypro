package Leetcode2.graph;

public class TestAMWGraph {

    public static void main(String[] args) {
        int n = 4, e = 4;
        String labels[] = {"v1", "v1", "v3", "v4", "v4"};
        AMWGraph graph = new AMWGraph(n);
        for (String label : labels) {
            graph.insertVertex(label);
        }

        graph.insertEdge(0, 1, 2);
        graph.insertEdge(0, 2, 5);
        graph.insertEdge(2, 3, 8);
        graph.insertEdge(3, 0, 7);

        System.out.println("vertex num is :" + graph.getnumOfVertex());
        System.out.println("edge num is :" + graph.getNumOfEdges());

        graph.deleteEdge(0, 1);
        System.out.println("delete < v1 v2> :" + graph.getnumOfVertex());
        System.out.println("delete < v1 v2> :" + graph.getNumOfEdges());


    }

}
