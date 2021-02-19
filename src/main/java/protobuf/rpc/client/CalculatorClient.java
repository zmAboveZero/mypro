package protobuf.rpc.client;

import org.apache.spark.rpc.netty.NettyRpcHandler;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class CalculatorClient {
    public static void main(String[] args) {
        NioClientSocketChannelFactory channelFactory =
                new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool());
        ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
        Channel channel = bootstrap.connect(new InetSocketAddress("localhost", 8080)).awaitUninterruptibly().getChannel();


    }
}
