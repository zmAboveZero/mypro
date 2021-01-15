package netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;

public class NettyNioServer {
    public static void main(String[] args) throws InterruptedException {

        final ByteBuf buf = Unpooled.copiedBuffer("HI!", Charset.forName("UTF-8"));
        NioEventLoopGroup group = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(group).channel(NioServerSocketChannel.class)
                .localAddress(new InetSocketAddress(8888))
                .childHandler(


                        //方法
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) throws Exception {
                                ch.pipeline().addLast(


                                        //方法
                                        new ChannelInboundHandlerAdapter() {
                                            @Override
                                            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                                ctx.writeAndFlush(buf.duplicate()).addListener(ChannelFutureListener.CLOSE);
                                            }
                                        });


                            }
                        });


        System.out.println("1==================");
        b.bind();
//        ChannelFuture f = b.bind().sync();
//        ChannelFuture f = b.bind().sync();
        System.out.println("2==================");
//        f.channel().closeFuture().sync();
//        System.out.println("3==================");


    }

}
