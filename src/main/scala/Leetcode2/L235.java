package Leetcode2;


import java.util.ArrayList;

public class L235 {


    public TreeNode v1(TreeNode root, TreeNode p, TreeNode q) {
        ArrayList<TreeNode> a = getNodeInBST(root, p);
        ArrayList<TreeNode> b = getNodeInBST(root, q);
        int index = 0;
        while (index < Math.min(a.size(), b.size())) {
            if (a.get(index).value != b.get(index).value) {
                return a.get(index - 1);
            }
        }
        return a.get(index - 1);
    }

    public ArrayList getNodeInBST(TreeNode root, TreeNode p) {
        ArrayList list = new ArrayList();
        while (root.value != p.value) {
            list.add(root);
            if (root.value > p.value) {
                root = root.left;
            } else {
                root = root.right;
            }
        }
        return list;
    }


    public class TreeNode {
        int value;
        TreeNode left;
        TreeNode right;

        TreeNode(int x) {
            value = x;
        }
    }

}
