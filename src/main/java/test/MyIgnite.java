package test;

import org.apache.commons.lang.time.StopWatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MyIgnite {
    public static void main(String[] args) {
        new MyIgnite().start();
    }

    private void start() {
        IgniteConfiguration icfg = new IgniteConfiguration();
        icfg.setIgniteInstanceName("test1");
        CacheConfiguration cacheConfig = new CacheConfiguration("test");
        cacheConfig.setCacheMode(CacheMode.LOCAL);
        cacheConfig.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC);
        icfg.setCacheConfiguration(cacheConfig);
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.setWalFsyncDelayNanos(10000);
//        storageCfg.getDataRegionConfigurations()[0].setPersistenceEnabled(false);
        icfg.setDataStorageConfiguration(storageCfg);
        Ignite ignite1 = Ignition.start(icfg);

        final CountDownLatch latch = new CountDownLatch(2);

        final int elements = 1000;
        final int queueSize = 50;

        ignite1.compute().runAsync(new IgniteRunnable() {

            @IgniteInstanceResource
            Ignite ignite;

            @Override
            public void run() {
                IgniteQueue<Double> queue = ignite.queue("test", queueSize, new CollectionConfiguration());
                System.out.println("test1 fetched queue");
                latch.countDown();
                try {
                    // wait until other runnable is able to poll
                    latch.await(5, TimeUnit.SECONDS);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                System.out.println("start test1");
                double test = 2;
                Random r = new Random();
                StopWatch sw = new StopWatch();
                sw.start();
                for (int i = 0; i < elements; i++) {
                    queue.put(r.nextDouble());
                }
                sw.stop();
                //LoggerFactory.getLogger(getClass()).info
                System.out.println("end test1. " + test + " at ignite " + ignite.name() + ", took:" + sw.getTime() / 1000f);
            }
        });

        System.out.println("starting test2");
        icfg = new IgniteConfiguration();
        icfg.setIgniteInstanceName("test2");
        Ignite ignite2 = Ignition.start(icfg);
        ignite2.compute().runAsync(new IgniteRunnable() {
            @IgniteInstanceResource
            Ignite ignite;

            @Override
            public void run() {
                IgniteQueue<Double> queue = ignite.queue("test", 0, null);
                System.out.println("test2 fetched queue");
                latch.countDown();
                try {
                    // wait until other runnable is able to poll
                    latch.await(10, TimeUnit.SECONDS);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
                System.out.println("start test2");
                StopWatch sw = new StopWatch();
                sw.start();
                int counter = 0;
                try {
                    for (int i = 0; i < elements; i++) {
                        Double res = queue.poll(5, TimeUnit.SECONDS);
                        counter++;
                    }

                } catch (IgniteException exc) {
                    System.out.println("Somehow cannot poll. " + exc);
                }
                sw.stop();
                //LoggerFactory.getLogger(getClass()).info
                System.out.println("end test2. counter " + counter + " at ignite " + ignite.name() + ", took:" + sw.getTime() / 1000f);
            }
        });

        System.out.println("oldest node: " + ignite1.cluster().forOldest().hostNames());
        System.out.println("nodes: " + ignite1.cluster().nodes().size());

        // does it really gracefully shut the nodes down?
//        Ignition.stop(ignite1.name(), false);
//        Ignition.stop(ignite2.name(), false);
    }
}
