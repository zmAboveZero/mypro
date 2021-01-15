package graph.pr1;

import java.util.ArrayList;
public class Node {
    public int nodeId;
    private ArrayList<Integer> neighbors = new ArrayList<Integer>();//以邻接表的形式表示图结构【无向图】
    private double pr = 1;
    public Node(int nodeId) {
        this.nodeId = nodeId;
    }
    public int getDegree() {
        return this.neighbors.size();
    }
    public ArrayList<Integer> getNeighbors() {
        return this.neighbors;
    }
    public void setNeighbors(ArrayList<Integer> neighbors) {
        this.neighbors = neighbors;
    }
    public double getPR() {
        return pr;
    }

    public void setPR(double val) {
        this.pr = val;
    }
    // 按PageRank值排序
    public int compareTo(Node anotherNode) {
        if (this.neighbors != null && anotherNode.neighbors != null) {
            // 降序排列
            if (anotherNode.getPR() > this.getPR())
                return 1;
            else if (anotherNode.getPR() < this.getPR())
                return -1;
            else
                return 0;
            // 升序排列
            // return this.getPR()-anotherNode.getPR();
        }
        return 0;
    }
}
