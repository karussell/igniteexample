package test;

import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class IgniteTest {
    public static void main(String... args) throws InterruptedException {
        Ignite ignite = getIgnite();

        IgniteQueue queue = getQueue(ignite);

        IgniteCluster cluster = ignite.cluster();

        IgniteEvents events = ignite.events();
        AtomicLong messageCounter = new AtomicLong();
        IgniteAtomicLong counter = ignite.atomicLong("counter", 0, true);

        ExecutorService consumerThread = Executors.newSingleThreadExecutor();
        consumerThread.execute(() -> {
            while (true) {
                System.out.println("Number taken: " + queue.take());
                System.out.println("The consumer takes " + messageCounter.incrementAndGet() + " messages");
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        events.localListen(event -> {
            if (cluster.forOldest().node().isLocal()) {
                System.out.println("Clearing...");
                queue.clear();
                System.out.println("I'm oldest!!");
            }
            return true;
        }, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT);
        Runtime.getRuntime().addShutdownHook(new Thread(consumerThread::shutdownNow));
        while (true) {
            if (cluster.forOldest().node().isLocal()) {
                long current = counter.getAndIncrement();
                System.out.println("Number put: " + current);
                queue.put(current);
                Thread.sleep(200);
            }
        }

    }

    private static IgniteQueue getQueue(Ignite ignite) {
        CollectionConfiguration cfg = new CollectionConfiguration();
        cfg.setBackups(1);
        cfg.setCollocated(false);
        return ignite.queue("queue", 0, cfg);
    }

    private static Ignite getIgnite() {
        IgniteConfiguration configuration = new IgniteConfiguration();
        TcpCommunicationSpi communicationSpi = new TcpCommunicationSpi();
        communicationSpi.setLocalPort(47200);
        communicationSpi.setLocalPortRange(2);
        TcpDiscoveryVmIpFinder tcpDiscoveryVmIpFinder = new TcpDiscoveryVmIpFinder();
        tcpDiscoveryVmIpFinder.setAddresses(Arrays.asList("localhost:47100..47101"));
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setIpFinder(tcpDiscoveryVmIpFinder);
        discoverySpi.setLocalPort(47100);
        discoverySpi.setLocalPortRange(2);
        configuration.setDiscoverySpi(discoverySpi);
        configuration.setCommunicationSpi(communicationSpi);
        configuration.setGridName("PRICE_CHANGE_ALERTS_GRID");
        configuration.setIncludeEventTypes(EventType.EVT_NODE_FAILED, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT);
        AtomicConfiguration atomicConfiguration = new AtomicConfiguration();
        atomicConfiguration.setCacheMode(CacheMode.REPLICATED);
        configuration.setAtomicConfiguration(atomicConfiguration);
        return Ignition.start(configuration);
    }
}
