package zk.sheeppen;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.simple.JSONObject;

import zk.sheeppen.sheep.BlackSheep;
import zk.sheeppen.sheep.Sheep;

/**
 * Sheepman is a zookeeper client in order to manage the Sheep Pen (remote task)
 * 
 * @author qrtt1
 */
public class Sheepman {

    private static Logger logger = Logger.getLogger(Sheepman.class);
    public static final String PATH_SHEEPPEN = Sheepman.namespace();
    public static final String PATH_SHEEP = PATH_SHEEPPEN + ".sheep";
    private Info info = new Info();
    private Map<String, Sheep> sheepKeeper = new HashMap<String, Sheep>();
    
    static class Info {
        final protected String id = UUID.randomUUID().toString().replace("-", "");
        final JSONObject json = new JSONObject();

        @SuppressWarnings("unchecked")
        public Info() {
            json.put("id", id);
        }

        public String path() {
            return Sheepman.namespace() + "/INFO_" + id;
        }

        public byte[] data() {
            return json.toJSONString().getBytes();
        }

        public String cmd() {
            return Sheepman.namespace() + "/BAA_" + id;
        }
    }
    

    static class Sheeppen extends Thread implements Watcher {
        final private ZooKeeper keeper;
        final private Sheepman sheepman;
        final private String watchedPath;
        private boolean terminated = false;

        public Sheeppen(String serverList, Sheepman sheepman) throws IOException,
                KeeperException, InterruptedException {
            this.sheepman = sheepman;
            keeper = new ZooKeeper(serverList, 10 * 1000, this);
            watchedPath = sheepman.info.cmd();
            register(sheepman);
        }

        private void register(Sheepman sheepman) throws KeeperException, InterruptedException {
            /*
             * create sheeppen and sheeppen.sheep if needs.
             * */
            try {
                keeper.create(PATH_SHEEPPEN, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("create namespace " + PATH_SHEEPPEN + " for first time installation");
            } catch (Exception ignored) {
            }
            try {
                keeper.create(Sheepman.PATH_SHEEP, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (Exception ignored) {
            }

            /*
             * create info and command znode for Sheepman info will reflect the
             * state of our Sheepman cmd will drive Sheep to go from others
             */
            Info info = sheepman.info;
            keeper.create(info.path(), info.data(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            keeper.create(info.cmd(), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            doWatch();
        }

        private void doWatch() {
            try {
                keeper.exists(watchedPath, true);
            } catch (Exception e) {
                terminated = true;
            }
        }

        public void process(WatchedEvent evt) {
            doWatch();
            if (evt.getType() == null && evt.getPath() == null) {
                logger.info("connected to zookeeper");
                return;
            }
            if (evt.getType() == EventType.NodeCreated) {
                /* ignore the create with empty data */
                return;
            }

            if (!StringUtils.contains(evt.getPath(), "/BAA_")) {
                return;
            }
            
            try {
                SheepBaa baa = SheepBaa.parse(new String(keeper.getData(evt.getPath(), true, null)));
                // TODO: global command
                if("global".equals(baa.getName()))
                {
                    if("stop_all".equals(baa.getMethod()))
                    {
                        sheepman.stopAll();
                    }
                    return ;
                }
                
                if("sheep".equals(baa.getMethod()))
                {
                    Sheep sheep = Sheep.create(keeper, Sheep.class, baa);
                    sheep.go(sheepman);
                }
                
                if("blacksheep".equals(baa.getMethod()))
                {
                    Sheep sheep = Sheep.create(keeper, BlackSheep.class, baa);
                    sheep.go(sheepman);
                }
                
                if("stop".equals(baa.getMethod()))
                {
                    // TODO stop 
                }
                
            } catch (KeeperException e) {
                terminated = true;
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
        }

        @Override
        public void run() {
            while (!terminated) {
                try {
                    Thread.sleep(1000);
                    keeper.exists("/", false);
                } catch (KeeperException e) {
                    logger.error(e.getMessage(), e);
                    break;
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    public void graze(String location) {

        try {
            Sheeppen sheeppen = new Sheeppen(location, this);
            sheeppen.start();
            sheeppen.join();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        // TODO: 處理 terminated

    }

    public void stopAll() {
            for (Sheep sheep : sheepKeeper.values()) {
                try {
                    logger.info("stop " + sheep);
                    sheep.stop();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
    }

    public static String namespace() {
        return "/" + Sheepman.class.getPackage().getName();
    }
    
    public void watch(Sheep sheep) {
        logger.info("watch: " + sheep);
        sheepKeeper.put(sheep.getSheepId(), sheep);
    }

    public void unwatch(Sheep sheep) {
        logger.info("unwatch: " + sheep);
        sheepKeeper.remove(sheep.getSheepId());
    }

}
