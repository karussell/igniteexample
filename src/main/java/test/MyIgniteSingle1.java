package test;

import org.apache.commons.lang.time.StopWatch;
import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MyIgniteSingle1 {
    // use?  -XX:MaxDirectMemorySize=500m
    public static void main(String[] args) {
        new MyIgniteSingle1().start();
        new MyIgniteSingle2().start();
    }

    void start() {
        CollectionConfiguration queueCfg = new CollectionConfiguration();
//        queueCfg.setCacheMode(CacheMode.PARTITIONED);
        final int queueSize = 100_000;
        final int elements = 100_000;

        IgniteConfiguration icfg = new IgniteConfiguration();
        icfg.setIgniteInstanceName("test1");
        final Ignite ignite1 = Ignition.start(icfg);
        IgniteCountDownLatch latch = ignite1.countDownLatch("latch", 1, true, true);
        ignite1.compute().runAsync(new IgniteRunnable() {

            @IgniteInstanceResource
            Ignite ignite;

            @Override
            public void run() {
                IgniteQueue<Double> queue = ignite.queue("test", queueSize, queueCfg);
                System.out.println("test1 fetched queue");

                double test = 2;
                StopWatch sw = new StopWatch();
                sw.start();
                Random r = new Random();

                for (int i = 0; i < elements; i++) {
                    queue.put(r.nextDouble());
                }
                latch.countDown();
                sw.stop();
                //LoggerFactory.getLogger(getClass()).info
                System.out.println("end test1. " + test + " at ignite " + ignite.name() + ", took:" + sw.getTime() / 1000f);
            }
        });

        new Thread() {
            @Override
            public void run() {

                latch.await(20, TimeUnit.SECONDS);

                IgniteQueue<Double> queue = ignite1.queue("test", 0, null);

                System.out.println(new Date() + " start test1-POLL");
                StopWatch sw = new StopWatch();
                sw.start();
                int counter = 0;
                try {
                    while (true) {
                        Double res = queue.take();
                        if (queue.isEmpty()) {
                            break;
                        }
                        counter++;
                        if(counter % 10000 == 0)
                            System.out.println(new Date() + " test1-POLL " + counter);
                    }

                } catch (IgniteException exc) {
                    System.out.println("Somehow cannot poll. " + exc);
                }
                sw.stop();
                //LoggerFactory.getLogger(getClass()).info
                System.out.println(new Date() + " end test1-POLL. counter " + counter + " at ignite " + ignite1.name() + ", took:" + sw.getTime() / 1000f);
            }
        }.start();

        System.out.println("oldest node: " + ignite1.cluster().forOldest().hostNames());
        System.out.println("nodes: " + ignite1.cluster().nodes().size());

        // does it really gracefully shut the nodes down?
//        Ignition.stop(ignite1.name(), false);
//        Ignition.stop(ignite2.name(), false);
    }
}
