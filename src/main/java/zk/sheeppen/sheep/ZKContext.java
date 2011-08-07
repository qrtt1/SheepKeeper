package zk.sheeppen.sheep;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZKContext {

    private final static Logger logger = Logger.getLogger(ZKContext.class);
    private ZooKeeper zk;

    public ZKContext(ZooKeeper keeper) {
        this.zk = keeper;
    }

    public ZKContext(String connectionString, Watcher watcher) throws IOException {
        this(new ZooKeeper(connectionString, 60 * 1000, watcher));
    }

    public String createTmpSequence(String zkPath, byte[] data) throws KeeperException,
            InterruptedException {
        return zk.create(Path.normalize(zkPath), data == null ? new byte[0] : data,
                Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public Stat stat(String zkPath) throws KeeperException, InterruptedException {
        return zk.exists(Path.normalize(zkPath), false);
    }

    public boolean exists(String zkPath) throws KeeperException, InterruptedException {
        return zk.exists(zkPath, false) != null;
    }

    public boolean delete(String zkPath) throws KeeperException, InterruptedException {
        zkPath = Path.normalize(zkPath);
        Stat stat = stat(zkPath);
        if (stat == null)
            return false;
        zk.delete(zkPath, stat.getVersion());
        return true;
    }

    public Stat update(String zkPath, byte[] data) throws KeeperException, InterruptedException {
        String normalPath = Path.normalize(zkPath);
        if (normalPath == null)
            return null;

        for (String path : new Path(zkPath)) {
            boolean isTarget = normalPath.equals(path);
            boolean createTarget = false;
            Stat stat = zk.exists(path, false);
            if (stat == null) {
                byte[] value = isTarget ? data : new byte[0];
                zk.create(path, value, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                createTarget = true;
            }
            if (isTarget && !createTarget && stat != null) {
                zk.setData(path, data == null ? new byte[0] : data, stat.getVersion());
                return stat(normalPath);
            }
        }
        return stat(normalPath);
    }

    public void deleteTree(String zkPath) throws KeeperException, InterruptedException {
        String nPath = Path.normalize(zkPath);
        if (nPath == null || !exists(nPath))
            return;

        List<String> children = zk.getChildren(nPath, false);
        if (!children.isEmpty()) {
            for (String cPath : children) {
                deleteTree(nPath + "/" + cPath);
            }
        }
        logger.info("tree-remove path: " + nPath);
        delete(nPath);
    }

    public boolean hasChildren(String zkPath) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(Path.normalize(zkPath), false);
        if (stat != null) {
            logger.info(zkPath + " have children: " + stat.getNumChildren());
            return stat.getNumChildren() != 0;
        }
        return false;
    }
    
    public String child(String path) throws KeeperException, InterruptedException
    {
        String zkPath = Path.normalize(path);
        List<String> children = zk.getChildren(zkPath, false);
        if (children != null && !children.isEmpty()) {
            Collections.shuffle(children);
            return zkPath + "/" + children.get(0);
        }
        return null;
    }
    
    public void touch(String path) throws KeeperException, InterruptedException
    {
        String zkPath = Path.normalize(path);
        update(zkPath, null);
    }
    
    public void move(String oldPath, String newPath) throws KeeperException, InterruptedException
    {
        byte[] data = zk.getData(Path.normalize(oldPath), false, null);
        delete(oldPath);
        update(Path.normalize(newPath), data);
        logger.info(String.format("move data from %s to %s", oldPath, newPath));
    }
    
    public String str(String path) throws KeeperException, InterruptedException
    {
        return new String(zk.getData(Path.normalize(path), false, null));
    }
    
    public void save(String path, Object data) throws KeeperException, InterruptedException
    {
        if(data instanceof byte[])
        {
            update(Path.normalize(path), (byte[]) data);
            return ;
        }
        update(Path.normalize(path), data != null ? data.toString().getBytes() : new byte[0]);
    }
    
    public void rm(String path) throws KeeperException, InterruptedException
    {
        delete(path);
    }
    
    public Integer age(String path) throws KeeperException, InterruptedException {
        update("/tmp", null);
        String tmpPath = zk.create("/tmp/" + System.currentTimeMillis(), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        try {
            return (int) (stat(tmpPath).getCtime()- stat(path).getCtime()) / 1000;    
        } catch (Exception e) {
            return null;
        }finally
        {
            delete(tmpPath);
        }
        
    }

}
