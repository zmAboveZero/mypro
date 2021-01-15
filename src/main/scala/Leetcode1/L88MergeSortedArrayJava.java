package Leetcode1;

public class L88MergeSortedArrayJava {


    public static void main(String[] ast) {

        int[] nums1 = new int[]{1, 2, 3, 0, 0, 0, 0, 0};
        int[] nums2 = new int[]{5, 6, 7};
        merge(nums1, 3, nums2, 3);
        int index = 0;
        int index2 = 0;
        int index3 = 0;


        index2 = index3++;

        System.out.println(index3);
//        System.out.println(index3++);//index3未被使用所以一直是0
//        System.out.println(index3);//index3被使用了一次所以加1
//        System.out.println(++index2);//index2在使用前加一

        while (index <= nums1.length - 1) {

//            System.out.println(nums1[index]);
            index++;
//            System.out.println(++index2);
//            System.out.println("=====================");
//            System.out.println(index3++);
        }
    }

    public static void merge(int[] nums1, int m, int[] nums2, int n) {
        // two get pointers for nums1 and nums2
        int p1 = m - 1;
        int p2 = n - 1;
        // set pointer for nums1
        int p = m + n - 1;

        // while there are still elements to compare
        while ((p1 >= 0) && (p2 >= 0))
            // compare two elements from nums1 and nums2
            // and add the largest one in nums1
            nums1[p--] = (nums1[p1] < nums2[p2]) ? nums2[p2--] : nums1[p1--];

        // add missing elements from nums2
        System.arraycopy(nums2, 0, nums1, 0, p2 + 1);
    }


}
