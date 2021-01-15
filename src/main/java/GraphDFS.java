//
//public class GraphDFS {
//    public Integer numVertex;
//    public Integer numEdge;
//    public VertexNode[] vertexNodes;
//    public boolean[] vst;
//    public Integer[] pre;
//    public boolean ringFinded;
//    public Integer[] numLayerNode;
//    public Integer[] nodeLyaer;
//
//    public GraphDFS(VertexNode[] vertexNodes) {
//        this.numEdge = 0;
//        this.numVertex = vertexNodes.length;
//        this.vertexNodes = vertexNodes;
//        vst = new boolean[numVertex];
//        pre = new Integer[numVertex];
//
//        for (int i = 0; i < pre.length; i++) {
//            pre[i] = -1;
//        }
//    }
//
//    public void insertEdge(Integer start, Integer end) {
//        VertexNode vertexNode = vertexNodes[start];
//        EdgeNode edgeNode = new EdgeNode(end);
//
//        EdgeNode firstEdgeNode = vertexNode.firstEdge;
//        if (firstEdgeNode == null) {
//            vertexNode.firstEdge = edgeNode;
//        } else {
//            edgeNode.next = firstEdgeNode;
//            vertexNode.firstEdge = edgeNode;
//        }
//    }
//
//    public void dfs(int root){
//        VertexNode vertexNode = this.vertexNodes[root];
//        vst[root] = true;
//        System.out.print(vertexNode.data + " ");
//
//        EdgeNode currentEdgeNode = vertexNode.firstEdge;
//
//        while (currentEdgeNode != null) {
//            int vertexNodeIndex = currentEdgeNode.adjvex;
//            if (vst[vertexNodeIndex] == false){
//                dfs(vertexNodeIndex);
//            }
//            currentEdgeNode = currentEdgeNode.next;
//        }
//    }
//
//    // 通过DFS找环
//    public void findRing(int root) {
//        vst[root] = true; // 标记为已访问
//
//        VertexNode vertexNode = vertexNodes[root]; 		 // 当前表头结点
//        EdgeNode currentEdgeNode = vertexNode.firstEdge;
//
//        while (currentEdgeNode != null && ringFinded == false) { // 遍历边结点，当找到环时结束
//            int vertexNodeIndex = currentEdgeNode.adjvex;
//
//            if (vst[vertexNodeIndex] == false){
//                pre[vertexNodeIndex] = root; 	// 记录父结点
//                findRing(vertexNodeIndex);				// 递归搜索子结点
//            }else if (pre[root] != vertexNodeIndex){
//                ringFinded = true;
//                backPath(root);
//                break;
//            }
//
//            currentEdgeNode = currentEdgeNode.next; 	// 搜索下一表头结点
//        }
//    }
//
//    // 回溯路径
//    public void backPath(int index){
//        while (index != -1){
//            System.out.println(vertexNodes[index].data);
//            index = pre[index];
//        }
//    }
//
//    public static void main(String[] args) {
//        VertexNode[] vertexNodes = {
//                new VertexNode(1,"a"),
//                new VertexNode(2,"b"),
//                new VertexNode(3,"c"),
//                new VertexNode(4,"d"),
//                new VertexNode(5,"e"),
//                new VertexNode(6,"f")
//        };
//
//        GraphDFS graph = new GraphDFS(vertexNodes);
//
//        graph.insertEdge(0, 2);
//        graph.insertEdge(0, 1);
//        graph.insertEdge(1, 4);
//        graph.insertEdge(1, 3);
//        graph.insertEdge(1, 0);
//        graph.insertEdge(2, 5);
//        graph.insertEdge(2, 4);
//        graph.insertEdge(2, 0);
//        graph.insertEdge(3, 1);
//        graph.insertEdge(4, 2);
//        graph.insertEdge(4, 1);
//        graph.insertEdge(5, 2);
//
//        graph.findRing(0);
//
////        打印邻接表结构
////        for (int i = 0; i < graph.numVertex; i++) {
////            VertexNode vertexNode = graph.vertexNodes[i];
////            EdgeNode firstEdge = vertexNode.firstEdge;
////
////            EdgeNode currentEdge = firstEdge;
////            System.out.print(vertexNode.data + ":");
////            while (currentEdge != null) {
////                int vertexNodeIndex = currentEdge.adjvex;
////                System.out.print("->" + vertexNodes[vertexNodeIndex].data);
////                currentEdge = currentEdge.next;
////            }
////            System.out.println();
////        }
//    }
//}
//
