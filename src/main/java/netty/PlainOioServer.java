package netty;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class PlainOioServer {
    private static BufferedReader br = null;
    private static PrintWriter pw = null;
    private static ServerSocket ss;
    private static Socket s;
    static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        try {
            ss = new ServerSocket(8888);
            System.out.println("服务器正常启动。。。。");

            while (true) {
                s = ss.accept();//阻塞方法
                System.out.println("连接成功" + s.getRemoteSocketAddress());
                br = new BufferedReader(new InputStreamReader(s.getInputStream()));
                pw = new PrintWriter(new OutputStreamWriter(s.getOutputStream()));
                for (; ; ) {
                    Thread.sleep(1000);
//                    System.out.println(index);
//                    System.out.println(br.readLine());
                    System.out.println("hhhhhhh");
                }

//                new Thread(new Runnable() {
//                    @Override
//                    public void run() {
//                        while (true) {
//                            System.out.println("inner");
//                            System.out.println(Thread.currentThread().getId());
//                            try {
//                                Thread.sleep(1000);
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                            String string = null;
////                            try {
////                                string = br.readLine();
////                            } catch (IOException e) {
////                                e.printStackTrace();
////                            }
//                            System.out.println("Server读到：" + string);
////                System.out.println("Server端请输入：");
////                String str = scanner.next();
////                pw.println("hello client");
////                pw.flush();
//                        }
//
//                    }
//                }).start();


//                System.out.println("haha");

            }


        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        try {
            pw.close();
            br.close();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
//        ServerSocket socket = new ServerSocket(8888);
//        for (; ; ) {
//            final Socket clientSocket = socket.accept();
//            new Thread(new Runnable() {
//                public void run() {
//                    System.out.println("Accept connection from " + clientSocket);
//                    OutputStream out;
//                    try {
//                        out = clientSocket.getOutputStream();
//                        out.write("hi\r\n".getBytes(Charset.forName("UTF-8")));
//                        out.flush();
//                        clientSocket.close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }).start();
//
//        }
    }
}
