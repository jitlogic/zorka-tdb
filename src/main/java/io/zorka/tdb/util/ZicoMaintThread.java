package io.zorka.tdb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZicoMaintThread implements Runnable {

    private final static Logger log = LoggerFactory.getLogger(ZicoMaintThread.class);

    private final Thread thread;
    private final ZicoMaintObject obj;
    private volatile boolean running;
    private final long interval;

    public ZicoMaintThread(String name, long interval, ZicoMaintObject obj) {
        this.obj = obj;

        this.running = true;
        this.interval = interval;

        this.thread = new Thread(this);
        this.thread.setName("ZICO-maint-" + name);
        this.thread.setDaemon(true);
        this.thread.start();
    }

    public synchronized void stop() {
        this.running = false;
    }

    private void runCycle() {
        try {
            if (!obj.runMaintenance()) {
                ZicoUtil.sleep(interval);
            }
        } catch (Exception e) {
            log.error("Error running maintenance task", e);
            ZicoUtil.sleep(interval);
        }
    }

    @Override
    public void run() {
        ZicoUtil.sleep(interval);
        while (running) {
            runCycle();
        }
    }

}
