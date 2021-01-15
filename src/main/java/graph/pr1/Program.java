package graph.pr1;

import java.util.ArrayList;

public class Program {
    public static void CalcPageRank(ArrayList<Node> graph) {
        double distance = 0.00001;
        double d = 0.85;// damping factor
        double common = (1 - d) / graph.size();
        while (true) {
            for (Node n : graph) {
                double sum = 0.0;
                for (int nodeId : n.getNeighbors()) {
                    Node nb = getNodeById(nodeId, graph);
                    sum += nb.getPR() / nb.getDegree();
                }
                double newPR = common + d * sum;
                //如果尚未收敛，赋新值，否则结束迭代
                if (Math.abs(n.getPR() - newPR) > distance)
                    n.setPR(newPR);
                else
                    return;
            }
        }
    }

    public static Node getNodeById(int nodeId, ArrayList<Node> graph) {
        for (Node n : graph) {
            if (n.nodeId == nodeId)
                return n;
        }
        return null;
    }


    public static ArrayList<Node> buildGraph() {
        ArrayList<Node> graph = new ArrayList<Node>();//图以节点集合形式来表示
        //加载数据，组装好图结构

        return graph;
    }

    public static void main(String[] args) {
        ArrayList<Node> graph = buildGraph();
        CalcPageRank(graph);
        for (Node n : graph) {
//            System.out.println("PageRank of %d is %.2f", n.nodeId, n.getPR());
            System.out.println(n.nodeId + ">>>>>>" + n.getPR());
        }
    }



}
