package com.coypan.hotitems_analysis;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class HotItems {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");  // 可不设置
        FlinkKafkaConsumer<String> consumer =  new FlinkKafkaConsumer<String>(
                "coypan",
                new SimpleStringSchema(),
                properties);

        DataStreamSource<String> sourceStream = env.addSource(consumer);

        DataStream<UserBehavior> dataStream = sourceStream.flatMap(new FlatMapFunction<String, UserBehavior>() {
            public void flatMap(String s, Collector<UserBehavior> collector) throws Exception {
                String[] splits = s.split(",");
                if (splits.length == 5) {
                    collector.collect(new UserBehavior(
                            Long.parseLong(splits[0]),
                            Long.parseLong(splits[1]),
                            Integer.parseInt(splits[2]),
                            splits[3],
                            Long.parseLong(splits[4])
                            ));
                }
            }
        }).assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(UserBehavior element) {
                        return element.timestamp;
                    }
                });

        DataStream<?> aggStream = dataStream.filter(new FilterFunction<UserBehavior>() {
            public boolean filter(UserBehavior userBehavior) throws Exception {
                if ("pv".equals(userBehavior.behavior))
                    return false;
                return true;
            }
        });

        aggStream.print("agg");

        env.execute("streaming word count");

    }

    static class UserBehavior {
        private long userId;
        private long itemId;
        private int categoryId;
        private String behavior;
        private long timestamp;

        public UserBehavior(long _userId, long _itemId, int _categoryId, String _behavior, long _timestamp) {
            this.userId = _userId;
            this.itemId = _itemId;
            this.categoryId = _categoryId;
            this.behavior = _behavior;
            this.timestamp = _timestamp;
        }
    }
}
