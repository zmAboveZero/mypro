package netty;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Scanner;

public class ClientTest {
    private static PrintWriter pw = null;
    private static BufferedReader br = null;
    private static Socket s;
    static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        try {
            Socket s = new Socket(InetAddress.getLocalHost(), 8889);
//            pw = new PrintWriter(new OutputStreamWriter(s.getOutputStream()));
//            br = new BufferedReader(new InputStreamReader(s.getInputStream()));
////            while (true) {
////            System.out.println("Client端请输入：");
////            String str = scanner.next();
//            int i = 0;
////            for (int index = 0; index < 10; index++) {
////                i++;
////                pw.println(i);
////                pw.flush();
////            }
////            while (true) {
////                Thread.sleep(500);
//
////            }
////            pw.println("hello server");HFileCleaner
////            pw.flush();
//            String string = br.readLine();
//            System.out.println("Client读到：" + string);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            br.close();
            pw.close();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }


    }
}
