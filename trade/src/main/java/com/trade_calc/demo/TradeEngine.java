package com.trade_calc.demo;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.map.EntryProcessor;
import com.trade_calc.demo.domain.Ticker;
import com.trade_calc.demo.util.MyProperties;
import org.apache.kafka.common.serialization.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;

public class TradeEngine implements Serializable {

    private HazelcastInstance hz;
    private static final String TXN_QUEUE_ID = "Queue_Source";

    private void start() {

        InputStream stream = null;
        try {
            stream = new FileInputStream(MyProperties.HAZELCAST_CONFIG);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (null == stream) {
            try {
                throw new FileNotFoundException("Hazelcast configuration not found. Exiting...");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            System.exit(-1);
        }

        Config config = Config.loadFromStream(stream);

        hz = Hazelcast.newHazelcastInstance(config);

        launchAllPipelines();
    }

    private static void readAllProps(String propFileLocation) {
        if(propFileLocation == null || propFileLocation.equals("")) {
            System.out.println("Properties not found, exiting..");
            System.exit(-1);
        }
        MyProperties.readAllProperties(propFileLocation);
    }

    private void launchAllPipelines() {
        launchFileReaderPipeline();
        launchQueueReaderPipeline();
        launchKafkaReaderPipeline();
    }

    private void launchQueueReaderPipeline() {
        Pipeline p = Pipeline.create();
        StreamStage<String> source = p.readFrom(buildQueueSource()).withoutTimestamps();
        StreamStage<Ticker> tickerStreamStage = source.map(value -> transformStringValue(value));

        tickerStreamStage.writeTo(Sinks.map("tickers", e -> e.getTicker(), e -> e));
        //tickerStreamStage.writeTo(Sinks.mapWithEntryProcessor("tickers", t -> t.getTicker(), t -> new UpdateEntryProcessor(t)));
        tickerStreamStage.writeTo(Sinks.logger(t -> String.format("Ticker %s from %s updated with new price %d on %s", t.getTicker(), t.getSource(), t.getPrice(), t.getTradeTime().toString())));

        JobConfig jobConfig = new JobConfig().setName("Ticker read from Queue");
        hz.getJet().newJobIfAbsent(p, jobConfig);
    }

    private void launchFileReaderPipeline() {
        Pipeline p = Pipeline.create();
        StreamStage<String> line = p.readFrom(Sources.fileWatcher("/Users/rahul/github-forks/micro-fin-configs/data")).withoutTimestamps();
        StreamStage<Ticker> tickerStreamStage = line.map(value -> transformStringValue(value));

        tickerStreamStage.writeTo(Sinks.map("tickers", e -> e.getTicker(), e -> e));
        //tickerStreamStage.writeTo(Sinks.mapWithEntryProcessor("tickers", t -> t.getTicker(), t -> new UpdateEntryProcessor(t)));
        tickerStreamStage.writeTo(Sinks.logger(t -> String.format("Ticker %s from %s updated with new price %d on %s", t.getTicker(), t.getSource(), t.getPrice(), t.getTradeTime().toString())));

        JobConfig jobConfig = new JobConfig().setName("Ticker read from File");
        hz.getJet().newJobIfAbsent(p, jobConfig);
    }

    private void launchKafkaReaderPipeline() {
        Pipeline p = Pipeline.create();
        StreamSourceStage<Map.Entry<Long, String>> readBloom = p.readFrom(KafkaSources.kafka(kafkaProps(), "bloomberg"));
        StreamStage<Ticker> bloomValue = readBloom.withIngestionTimestamps().rebalance().map(e -> transformStringValue(e.getValue()));

        StreamSourceStage<Map.Entry<Long, String>> readRtr = p.readFrom(KafkaSources.kafka(kafkaProps(), "reuter"));
        StreamStage<Ticker> rtrValue = readRtr.withIngestionTimestamps().rebalance().map(e -> transformStringValue(e.getValue()));

        StreamStage<KeyedWindowResult<String, Ticker>> max = bloomValue.merge(rtrValue)
                .groupingKey(e -> e.getTicker())
                .window(WindowDefinition.sliding(1000, 500))
                .aggregate(AggregateOperations.minBy(ComparatorEx.comparing(t -> t.getPrice())));

        max.writeTo(Sinks.logger());
        max.writeTo(Sinks.map("Kafka-Tickers", e -> e.getKey(), e -> e.getValue()));

        JobConfig jobCfg = new JobConfig().setName("Ticker Calculation from Kafka");
        jobCfg.addClass(TradeEngine.class);
        jobCfg.addClass(Ticker.class);

        hz.getJet().newJobIfAbsent(p, jobCfg);
    }

    private static Ticker transformStringValue(String value) {
        String tokens[] = value.split(",");

        String[] split = tokens[0].split("-");
        String tkr = split[1];
        String source = split[0];

        return new Ticker(tkr, Integer.parseInt(tokens[1]), source, tokens[2]);
    }

    private StreamSource<String> buildQueueSource() {
        StreamSource<String> source = SourceBuilder.<QueueContext<String>>stream(TXN_QUEUE_ID, c -> new QueueContext<>(c.hazelcastInstance().getQueue(TXN_QUEUE_ID)))
                .<String>fillBufferFn(QueueContext::fillBuffer)
                .build();

        return source;
    }

    static class QueueContext<T> extends AbstractCollection<T> {
        static final int MAX_ELEMENTS = 1024;
        IQueue<T> queue;
        SourceBuilder.SourceBuffer<T> buf;
        QueueContext(IQueue<T> queue) {
            this.queue = queue;
        }

        void fillBuffer(SourceBuilder.SourceBuffer<T> buf) {
            this.buf = buf;
            queue.drainTo(this, MAX_ELEMENTS);
        }
        @Override
        public boolean add(T item) {
            buf.add(item);
            return true;
        }
        @Override
        public Iterator<T> iterator() {
            throw new UnsupportedOperationException();
        }
        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }
    }

    static class UpdateEntryProcessor implements EntryProcessor<String, Ticker, Integer> {

        //private static int price;
        private Ticker newEntry;

        public UpdateEntryProcessor(Ticker newEntry) {
            if(newEntry == null)
                this.newEntry = newEntry;
        }

        @Override
        public Integer process(Map.Entry<String, Ticker> entry) {
            Ticker ticker = entry.getValue();

            //if this is a new entry
            if(entry.getValue() == null) {
                entry.setValue(newEntry);
                return 1;
            }

            ticker.setPrice(newEntry.getPrice());
            entry.setValue(ticker);
            return 1;
        }
    }

    private static Properties kafkaProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", LongDeserializer.class.getCanonicalName());
        props.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        props.setProperty("auto.offset.reset", "earliest");
        return props;
    }

    public static void main(String[] args) {
        if(args.length == 0 || args[0].equals("")) {
            System.out.println("Properties not defined. Please provide fully qualified path to the Process.properties... exiting");
            System.exit(-1);
        }
        readAllProps(args[0]);
        new TradeEngine().start();
    }
}
