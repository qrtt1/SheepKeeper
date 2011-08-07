package zk.sheeppen.sheep;

import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.GroovyShell;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import zk.sheeppen.SheepBaa;
import zk.sheeppen.Sheepman;

public class Sheep {

    static Logger logger = Logger.getLogger(Sheep.class);
    private final String id = UUID.randomUUID().toString().replace("-", "");
    protected Binding binding = new Binding();
    protected GroovyShell groovyShell = new GroovyShell(binding);
    protected SheepBaa baa;
    
    @SuppressWarnings("rawtypes")
    protected Closure sheepSoul;

    static enum STATE {
        PENDING, STARTED, STOPED
    }
    
    protected STATE state = STATE.PENDING;

    @SuppressWarnings("rawtypes")
    public Sheep(SheepBaa baa, byte[] data) {
        /* TODO: closure 能利用 Watcher 機制觀察，有更新再建新的，不需要每次都重建。 */
        groovyShell.evaluate(new String(data));
        sheepSoul = (Closure) binding.getProperty("app");
    }

    final public String getSheepId() {
        return id;
    }

    public void go(final Sheepman sheepman) {
        new Thread(Sheep.class.getSimpleName() + "_" + getSheepId()) {
            public void run() {
                Sheep.this.state = STATE.STARTED;
                try {
                    sheepman.watch(Sheep.this);
                    sheepSoul.call();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                } finally {
                    sheepman.unwatch(Sheep.this);
                    Sheep.this.state = STATE.STOPED;
                }
            }
        }.start();
    }
    
    public void stop()
    {
        state = STATE.STOPED;
    }
    
    public static Sheep create(ZooKeeper keeper, Class<? extends Sheep> sheepClass, SheepBaa baa) {
        try {
            Stat stat = keeper.exists(Sheepman.PATH_SHEEP + "/" + baa.getName(), false);
            if (stat == null) {
                throw new RuntimeException("no sheep at " + Sheepman.PATH_SHEEP + "/" + baa.getName());
            }

            /* get fetch Sheep data */
            String sheepDataPath = Sheepman.PATH_SHEEP + "/" + baa.getName();
            List<String> sheeps = keeper.getChildren(sheepDataPath, false);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (String s : new TreeSet<String>(sheeps)) {
                out.write(keeper.getData(sheepDataPath + "/" + s, false, null));
            }
            if(sheepClass != null)
            {
                Constructor<? extends Sheep> sheepConstructor = sheepClass.getConstructor(SheepBaa.class, byte[].class);
                return sheepConstructor.newInstance(baa, out.toByteArray());
            }
            return new Sheep(baa, out.toByteArray());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        throw new RuntimeException("no sheep at " + Sheepman.PATH_SHEEP + "/" + baa.getName());
    }

}
