package test;

import org.apache.commons.lang.time.StopWatch;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueueExample {
    public static void main(String[] args) {
        new QueueExample().start();
    }

    private void start() {
        final AtomicBoolean finishedTest1 = new AtomicBoolean(false);
        final BlockingQueue<Double> queue = new LinkedBlockingQueue<>(200_000);
        final CountDownLatch latch = new CountDownLatch(2);
        final int MAX = 400_000;

        new Thread(() -> {
            System.out.println("test1 before latch");
            latch.countDown();
            try {
                // wait until other runnable is able to poll
                latch.await(20, TimeUnit.SECONDS);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            System.out.println(new Date().getTime() + " start test1");
            double test = 2;
            Random r = new Random();
            StopWatch sw = new StopWatch();
            sw.start();
            for (int i = 0; i < MAX; i++) {
                try {
                    queue.put(r.nextDouble());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            sw.stop();
            finishedTest1.set(true);
            //LoggerFactory.getLogger(getClass()).info
            System.out.println(new Date().getTime() + " end test1. " + test + ", took:" + sw.getTime() / 1000f);
        }).start();

        new Thread(() -> {
            System.out.println("test2 before latch");
            latch.countDown();
            try {
                // wait until other runnable is able to poll
                latch.await(10, TimeUnit.SECONDS);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            System.out.println(new Date().getTime() + " start test2");
            StopWatch sw = new StopWatch();
            sw.start();
            int counter = 0;
            try {
                for (int i = 0; i < MAX ; i++) {
                    Double res = queue.poll(1, TimeUnit.SECONDS);
                    counter++;
                }
            } catch (InterruptedException e) {
                // expected
            }
            sw.stop();

            //LoggerFactory.getLogger(getClass()).info
            System.out.println(new Date().getTime() + " end test2. counter " + counter + ", finished:" + finishedTest1.get() + ", took:" + sw.getTime() / 1000f);
        }).start();
    }
}
