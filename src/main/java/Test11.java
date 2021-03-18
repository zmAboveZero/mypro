public class Test11 {
    public static void main(String[] args) {


        new Thread(new Runnable() {
            @Override
            public void run() {

                System.out.println("thread1");
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {

                System.out.println("thread2");
            }
        }).start();


        System.out.println("thread main");

    }
}
