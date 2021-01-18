public class Test3 {
    static int index=0;
    public static void main(String[] args) {
//        int i = 1;
//        for (i = 1; i <= 20; i++) {
//            System.out.println("兔子第" + i + "个月的总数为:" + f(i));
//
//
//
//        }

        int w = f(10);
        System.out.print(index);

    }

    public static int f(int x) {
        index++;
        if (x == 1 || x == 2) {
            return 1;
        } else {
            return f(x - 1) + f(x - 2);
        }
    }

}
