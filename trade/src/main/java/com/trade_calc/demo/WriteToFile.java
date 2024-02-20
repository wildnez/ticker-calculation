package com.trade_calc.demo;

import org.joda.time.DateTime;
import java.io.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;


public class WriteToFile {

    private static final String TXN_QUEUE_ID = "Queue_Source";

    public static void main(String[] args) {
        publish();
    }

    private static void publish() {

        System.out.println("Started writing in File");
        String sources[] = {"bloomberg", "reuter"};
        String trade_names[] = {"microsoft", "tesla", "bhp", "vanguard", "blackrock"};
        Random t_name_random = new Random(0);
        Random price_random = new Random(1);
        Random source_random = new Random(0);

        PrintWriter writer = null;
        try {
            writer = new PrintWriter("/Users/rahul/github-forks/micro-fin-configs/data/trades-new.txt");

            for(;;) {
                String trade_name = trade_names[t_name_random.nextInt(5)];
                int price = price_random.nextInt(10) * 100;
                String time = DateTime.now().toString();

                String trade = sources[source_random.nextInt(2)] + "-" + trade_name + "," + price + "," + time;

                writer.println(trade);
                writer.flush();
                TimeUnit.MILLISECONDS.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {

                writer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
