package netty;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class PlainNioServer {
    public static void main(String[] args) throws IOException {
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        ServerSocket ssocket = serverChannel.socket();
        ssocket.bind(new InetSocketAddress(8888));


        ServerSocketChannel serverChannel1 = ServerSocketChannel.open();
        serverChannel1.configureBlocking(false);
        ServerSocket ssocket1 = serverChannel1.socket();
        ssocket1.bind(new InetSocketAddress(8889));





        Selector selector = Selector.open();
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        serverChannel1.register(selector, SelectionKey.OP_ACCEPT);




        ByteBuffer msg = ByteBuffer.wrap("hi".getBytes());
//        for (; ; ) {
        while (true) {
            System.out.println("in---------");
            selector.select();
            System.out.println("out--------------");
            Set<SelectionKey> readKeys = selector.selectedKeys();
//            Systet.println("-------------" + readKeys.size());
            Iterator<SelectionKey> iterator = readKeys.iterator();
            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();
                System.out.println(key.channel().getClass());
                iterator.remove();
                System.out.println(selector.selectedKeys().size());
//                System.out.println(iterator.hasNext());

                if (key.isAcceptable()) {
                    ServerSocketChannel server = (ServerSocketChannel) key.channel();
                    SocketChannel client = server.accept();
                    client.configureBlocking(false);
                    client.register(selector, SelectionKey.OP_WRITE | SelectionKey.OP_READ, msg.duplicate());
//                    System.out.println("Accept connection from " + client);
                }
                if (key.isWritable()) {
                    SocketChannel client = (SocketChannel) key.channel();
                    ByteBuffer buffer = (ByteBuffer) key.attachment();
                    while (buffer.hasRemaining()) {
                        if (client.write(buffer) == 0) {
                            break;
                        }
                    }
                    client.close();
                }
            }
//            System.out.println(selector.selectedKeys().size() + "========");
//            selector.select();
//            System.out.println(selector.selectedKeys().size() + "+++++++++++++");
//            Iterator<SelectionKey> fsadf = selector.selectedKeys().iterator();
//            fsadf.next();
//            fsadf.remove();
//            System.out.println(selector.selectedKeys().size() + "************");

//            Set<SelectionKey> readKeys1 = selector.selectedKeys();
//            System.out.println("fsdafdsafas" + readKeys1.size());
//            Iterator<SelectionKey> iterator1 = readKeys1.iterator();

//            while (iterator1.hasNext()) {
//                SelectionKey keydsfs = iterator1.next();
//                iterator1.remove();
//                System.out.println("fsdafa");
//            }
//
//            selector.select();
//            System.out.println(selector.selectedKeys().size());
//            System.out.println("========");
//            System.out.println(selector.selectedKeys().size());
        }
    }
}
