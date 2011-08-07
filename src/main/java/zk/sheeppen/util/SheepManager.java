package zk.sheeppen.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import zk.sheeppen.Sheepman;

public class SheepManager {

    private static Logger logger = Logger.getLogger(SheepManager.class);
    private static String SHEEP_PATH = Sheepman.namespace() + ".sheep";
    private static Watcher EMPTY_WATCHER = new Watcher() {
        public void process(WatchedEvent evt) {
        }
    };
    
    private static String param(String name) {
        String v = System.getProperty(name);
        if (StringUtils.isEmpty(v))
            throw new IllegalArgumentException(String.format("%s property cannot be empty", name));
        return v;
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, FileNotFoundException {
        /*
mvn exec:java -Dexec.mainClass=zk.sheeppen.util.SheepManager \
-Dserver=127.0.0.1:2181 -Daction=list
*/
        String server = System.getProperty("server");
        String action = System.getProperty("action");
        
        ZooKeeper keeper = null;
        try {
            keeper = new ZooKeeper(server, 10 * 1000, EMPTY_WATCHER);
        } catch (Exception e) {
            logger.error("Failed to zookeeper server error", e);
            return ;
        }

        if ("list".equals(action)) {
            List<String> sheeps = keeper.getChildren(SHEEP_PATH, false);
            for (String s : sheeps) {
                System.out.println(SHEEP_PATH + "/" + s);
            }
        }
        
        if ("cat".equals(action)) {
            String base = SHEEP_PATH + "/" + param("sheep");
            List<String> sheeps = keeper.getChildren(base, false);
            for (String s : new TreeSet<String>(sheeps)) {
                System.out.print(new String(keeper.getData(base + "/" + s, false, null)));
            }
        }
        
        if ("install".equals(action)) {
            String base = SHEEP_PATH + "/" + param("sheep");
            String sheepFile = param("sheepFile");
            SplitInputStream in = new SplitInputStream(new FileInputStream(sheepFile));
            try {
                Stat stat = keeper.exists(base, false);
                if (stat != null)
                    throw new RuntimeException(String.format("%s is not empty", base));
                keeper.create(base, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                while (true) {
                    byte[] data = in.readNextSplit();
                    if (data == null)
                        break;
                    keeper.create(base + "/d", data, Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT_SEQUENTIAL);
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            } finally {
                try {
                    if (in != null)
                        in.close();
                } catch (Exception ignored) {
                }
            }
        }

        if ("uninstall".equals(action)) {
            String base = SHEEP_PATH + "/" + param("sheep");
            List<String> sheeps = keeper.getChildren(base, false);
            FileOutputStream backup = new FileOutputStream(param("sheep") + "_" + System.currentTimeMillis() + ".backup");
            try {
                for (String s : new TreeSet<String>(sheeps)) {
                    String dataPath = base + "/" + s;
                    backup.write(keeper.getData(dataPath, false, null));
                    keeper.delete(dataPath, -1);
                }
                keeper.delete(base, -1);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                try {
                    backup.close();
                } catch (IOException ignored) {
                }
            }
        }
        
        keeper.close();

    }
}
