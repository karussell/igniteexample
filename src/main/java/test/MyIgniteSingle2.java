package test;

import org.apache.commons.lang.time.StopWatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class MyIgniteSingle2 {
    void start() {
        System.out.println("starting test2");
        IgniteConfiguration icfg = new IgniteConfiguration();

        final int elements = 40_000;

        icfg.setIgniteInstanceName("test2");
        Ignite ignite2 = Ignition.start(icfg);
        ignite2.compute().runAsync(new IgniteRunnable() {
            @IgniteInstanceResource
            Ignite ignite;

            @Override
            public void run() {
                IgniteQueue<Double> queue = ignite.queue("test", 0, null);
                System.out.println("test2 fetched queue");

                int counter = 0;
                try {
                    for (int i = 0; i < elements / 4; i++) {
                        Double res = queue.poll(5, TimeUnit.SECONDS);
                        counter++;
                    }

                } catch (IgniteException exc) {
                    System.out.println("Somehow cannot poll. " + exc);
                }
                System.out.println(new Date() + " warmed test2");

                try {
                    // wait until other runnable is able to poll
                    Thread.sleep(2000);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }

                System.out.println("start test2");
                StopWatch sw = new StopWatch();
                sw.start();
                counter = 0;
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

        // does it really gracefully shut the nodes down?
//        Ignition.stop(ignite1.name(), false);
//        Ignition.stop(ignite2.name(), false);
    }
}
