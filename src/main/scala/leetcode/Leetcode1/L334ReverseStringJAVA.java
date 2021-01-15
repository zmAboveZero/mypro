package leetcode.Leetcode1;

public class L334ReverseStringJAVA {


    public static void main(String[] args) {

        String s = "hello";
        char[] ch = s.toCharArray();
        int i = 0;
        int j = s.length() - 1;
        while (i < j) {
            char temp = ch[i];
            ch[i] = ch[j];
            ch[j] = temp;
            i++;
            j--;
        }

        System.out.print(ch);


    }


}
