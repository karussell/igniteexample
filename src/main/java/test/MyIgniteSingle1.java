package test;

import org.apache.commons.lang.time.StopWatch;
import org.apache.ignite.Ignite;
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

import java.util.Date;
import java.util.Random;

public class MyIgniteSingle1 {
    // use?  -XX:MaxDirectMemorySize=500m
    public static void main(String[] args) {
        new MyIgniteSingle1().start();
        new MyIgniteSingle2().start();
    }

    void start() {
        CollectionConfiguration queueCfg = new CollectionConfiguration();
        final int queueSize = 20_000;
        final int elements = 40_000;

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

        ignite1.compute().runAsync(new IgniteRunnable() {

            @IgniteInstanceResource
            Ignite ignite;

            @Override
            public void run() {
                IgniteQueue<Double> queue = ignite.queue("test", queueSize, queueCfg);
                System.out.println("test1 fetched queue");

                try {
                    // wait until other runnable is able to poll
                    Thread.sleep(3000);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }

                Random r = new Random();
                for (int i = 0; i < elements / 4; i++) {
                    queue.put(r.nextDouble());
                }

                System.out.println(new Date() + " warmed test1");
                try {
                    // wait until other runnable is able to poll
                    Thread.sleep(3000);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }

                System.out.println("start test1");
                double test = 2;
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

        System.out.println("oldest node: " + ignite1.cluster().forOldest().hostNames());
        System.out.println("nodes: " + ignite1.cluster().nodes().size());

        // does it really gracefully shut the nodes down?
//        Ignition.stop(ignite1.name(), false);
//        Ignition.stop(ignite2.name(), false);
    }
}
