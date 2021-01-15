package Leetcode1;


import static java.lang.Math.abs;

public class L02ReverseIntegerJava {

    public static void main(String[] args) {

//        System.out.print(run());
        System.out.print(run2());
    }

    public static Integer run2() {
        Integer x = -12356546;
        int ret = 0;
        while (x != 0) {
            int temp = ret * 10 + x % 10;
            if (temp / 10 != ret)
                return 0;
            ret = temp;
            x /= 10;
        }
        return ret;

    }


    public static Integer run() {
        Integer x = -123;
        if (x > Integer.MAX_VALUE || x <= Integer.MIN_VALUE) {
            return 0;
        }
        char[] chars = String.valueOf(abs(x)).toCharArray();
        Integer start = 0;
        Integer end = chars.length - 1;

        while (start < end) {
            char temp = chars[start];
            chars[start] = chars[end];
            chars[end] = temp;
            start++;
            end--;
        }


        Long re = Long.valueOf(String.valueOf(chars));

        if (x < 0) {
            re = -re;
        }
        if (re > Integer.MAX_VALUE || re <= Integer.MIN_VALUE) {
            re = 0L;
        }


        return re.intValue();
    }


}
