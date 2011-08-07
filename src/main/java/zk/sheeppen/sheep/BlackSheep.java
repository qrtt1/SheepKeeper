package zk.sheeppen.sheep;

import groovy.lang.Closure;

import java.util.Vector;

import org.apache.commons.lang.math.NumberUtils;

import zk.sheeppen.SheepBaa;
import zk.sheeppen.Sheepman;

public class BlackSheep extends Sheep {
    
    Vector<Thread> workers = new Vector<Thread>();
    Object mutex = new Object();
    int workerLimit = 1;

    public BlackSheep(SheepBaa baa, byte[] data) {
        super(baa, data);
        if (baa.getArgs() != null && baa.getArgs().length >= 1) {
            int v = NumberUtils.toInt(baa.getArgs()[0], 1);
            if (v > 0) {
                workerLimit = v;
            }
        }
    }
    
    @SuppressWarnings("rawtypes")
    static class Worker extends Thread{
        private BlackSheep sheep;
        private Closure closure;
        
        public Worker(BlackSheep sheep, Closure closure) {
            this.sheep = sheep;
            this.closure = closure;
        }
        
        @Override
        public void run() {
            
            try {
                synchronized (sheep.mutex) {
                    sheep.workers.add(this);
                }
                closure.call();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            finally{
                synchronized (sheep.mutex) {
                    sheep.workers.remove(this);
                    sheep.mutex.notifyAll();
                }
            }
        }
    }

    public void togother(Sheepman sheepman)
    {
        state = Sheep.STATE.STARTED;
        sheepman.watch(this);
        try {
            while(!state.equals(STATE.STOPED))
            {
                synchronized (mutex) {
                    while(workers.size() >= 2)
                    {
                        mutex.wait();
                    }
                }
                new Worker(this, sheepSoul).start();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            state = Sheep.STATE.STOPED;
            sheepman.unwatch(this);
        }
    }
    
    @Override
    public void go(final Sheepman sheepman) {
        new Thread(BlackSheep.class.getSimpleName() + "_" + getSheepId()) {
            public void run() {
                togother(sheepman);
            }
        }.start();
    }

}
