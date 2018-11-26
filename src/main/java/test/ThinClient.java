package test;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThinClient {
    Logger logger = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        new ThinClient().start();
    }

    private void start() {
        logger.info("Simple Ignite thin client example working over TCP socket.");
        ClientConfiguration cfg = new ClientConfiguration().setAddresses("localhost:1234");
        try (IgniteClient igniteClient = Ignition.startClient(cfg)) {
            ClientCache<String, String> clientCache = igniteClient.getOrCreateCache("test");

            clientCache.put("Moscow", "095");
            clientCache.put("Vladimir", "033");

            String val = clientCache.get("Vladimir");
            logger.info("Print value: {}", val);
        } catch (IgniteException e) {
            logger.error("Ignite exception:", e.getMessage());
        } catch (Exception e) {
            logger.error("Ignite exception:", e.getMessage());
        }
    }
}
