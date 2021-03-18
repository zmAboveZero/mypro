public class GCDemo {
    public static void main(String[] args) {
        // allocate 4M space
        byte[] b = new byte[4 * 1024 * 1024];
        System.out.println("first allocate");
        // allocate 4M space
        b = new byte[4 * 1024 * 1024];
        System.out.println("second allocate");


        Thread myThread0 = new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    while (true) {
                        Thread.sleep(2000);
                        System.out.println("child thread 0");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        myThread0.setName("myThread0");
        myThread0.start();

        Thread myThread1 = new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    while (true) {
                        Thread.sleep(2000);
                        System.out.println("child thread 1");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        myThread1.setName("myThread1");
        myThread1.start();




//        while (true) {
//
//            try {
//
//                System.out.println("----------");
//                Thread.sleep(2000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//
//        }
    }


}
