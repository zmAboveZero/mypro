package test;

public class Test7 {
    static int counter=0;

    public static void main(String[] args) {
        System.out.println("第40位是" + show(40));
        System.out.print(counter);

    }

    public static int show(int i) {
        counter++;
        if (i < 0) {
            return 0;
        } else if (i > 0 && i < 2) {
            return 1;
        }

        return show(i - 1) + show(i - 2);
    }


}
