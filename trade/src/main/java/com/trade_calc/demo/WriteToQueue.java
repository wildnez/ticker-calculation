package com.trade_calc.demo;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;

import org.joda.time.DateTime;

import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class WriteToQueue {

    private static final String TXN_QUEUE_ID = "Queue_Source";

    public static void main(String[] args) throws SQLException, InterruptedException{
        publish();
    }

    private static void publish() throws InterruptedException {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("localhost:5701");
        config.setClusterName("dev");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IQueue queue = client.getQueue(TXN_QUEUE_ID);

        String sources[] = {"bloomberg", "reuter"};
        String trade_names[] = {"microsoft", "tesla", "bhp", "vanguard", "blackrock"};
        Random t_name_random = new Random(0);
        Random price_random = new Random(1);
        Random source_random = new Random(0);

        for(;;) {
            String trade_name = trade_names[t_name_random.nextInt(5)];
            int price = price_random.nextInt(10) * 100;
            String time = DateTime.now().toString();

            String trade = sources[source_random.nextInt(2)] + "-" + trade_name + "," + price + "," + time;
            queue.offer(trade);

            System.out.println("Published "+ trade);
            try {
                TimeUnit.MILLISECONDS.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
