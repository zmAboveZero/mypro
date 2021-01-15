package test;

import java.util.ArrayList;

public class Test5 {
    public static void main(String[] args) {
        int index = 0;
        ArrayList<String> arr = new ArrayList<String>();
        String v = "hello";
        try {


            while (true) {
                arr.add(v);
                v += v;
                index++;
                Thread.sleep(5000);
            }
        } catch (Exception e) {

        } finally {
            System.out.println(index);
        }

    }
}
