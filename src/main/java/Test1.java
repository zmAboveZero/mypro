public class Test1 {
    public static void main(String[] args) {


        Thread thread = new Thread(new Runnable() {
            public void run() {

                while (true) {

//                    try {
////                        Thread.sleep(Long.valueOf(1000));
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    System.out.println("dfsd");
                }
            }
        });


        System.out.println("========================");
        thread.setName(">>>>>>>>>>>>>>>>>>>name");
//        thread.start();


        while (true) {
            new Test2();
        }


    }
}
