package Leetcode1;

import Leetcode1.bean.ListNode;

public class L21MergeTwoSortedListsJava {

    public static void main(String[] args) {

//        Integer[] l1 =[1, 2, 4];
//        Integer[] l2 = [1, 3, 4];


    }





    public ListNode mergeTwoLists(ListNode l1, ListNode l2) {
        if (l1 == null) {
            return l2;
        } else if (l2 == null) {
            return l1;
        } else if (l1.val < l2.val) {
            l1.next = mergeTwoLists(l1.next, l2);
            return l1;
        } else {
            l2.next = mergeTwoLists(l1, l2.next);
            return l2;
        }

    }


}
