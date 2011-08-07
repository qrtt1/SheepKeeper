package zk.sheeppen.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import zk.sheeppen.Sheepman;

public class SheepMaker {

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        String server = "127.0.0.1:2181";
        String sheepPath = Sheepman.namespace() + ".sheep";

        ZooKeeper keeper = new ZooKeeper(server, 10 * 1000, new Watcher() {
            public void process(WatchedEvent arg0) {
            }
        });
        
        String appPath = sheepPath + "/abc";
        String dataPath = appPath + "/d";
        keeper.create(appPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
        SplitInputStream in = new SplitInputStream(new FileInputStream("/Users/qrtt1/list.txt"));
        while (true) {
            byte[] data = in.readNextSplit();
            if (data == null)
                break;
            
            keeper.create(dataPath, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        }
        in.close();
        
        Thread.sleep(1000);

        // keeper.create(sheepPath, data, acl, createMode)
    }

}
