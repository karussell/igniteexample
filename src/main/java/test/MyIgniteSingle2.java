package test;

import org.apache.commons.lang.time.StopWatch;
import org.apache.ignite.*;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class MyIgniteSingle2 {
    public static void main(String[] args) {
        new MyIgniteSingle2().start();
    }

    void start() {
        System.out.println("starting test2");

        final int elements = 40_000;

        IgniteConfiguration icfg = new IgniteConfiguration();
        icfg.setIgniteInstanceName("test2");
        final Ignite ignite = Ignition.start(icfg);
        IgniteCountDownLatch latch = ignite.countDownLatch("latch", 1, false, false);
        new Thread() {
            @Override
            public void run() {
                IgniteQueue<Double> queue = ignite.queue("test", 0, null);
                System.out.println("test2 fetched queue");

                latch.await(20, TimeUnit.SECONDS);

                System.out.println(new Date() + " start test2");
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
                        if (counter % 10000 == 0)
                            System.out.println(new Date() + " test2 " + counter);
                    }

                } catch (IgniteException exc) {
                    System.out.println("Somehow cannot poll. " + exc);
                }
                sw.stop();
                //LoggerFactory.getLogger(getClass()).info
                System.out.println(new Date() + " end test2. counter " + counter + " at ignite " + ignite.name() + ", took:" + sw.getTime() / 1000f);
            }
        }.start();

        // does it really gracefully shut the nodes down?
//        Ignition.stop(ignite1.name(), false);
//        Ignition.stop(ignite2.name(), false);
    }
}
