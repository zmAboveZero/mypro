import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZKTest {
    final static CountDownLatch connectedSignal = new CountDownLatch(1);


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        String path = "/nihoadsfasdfasdfsdafsdafsda"; // Assign path to znode
        byte[] data = "My first zookeeper app".getBytes(); // Declare data
        ZooKeeper zk = new ZooKeeper("localhost", 3000, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        System.out.println(zk.getSessionId());
        System.out.println(zk.getState());


    }

}
