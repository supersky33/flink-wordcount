package com.coypan.hotitems_analysis;

import domain.ItemViewCount;
import domain.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

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
                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        DataStream<ItemViewCount> aggStream = dataStream
                .filter(new FilterFunction<UserBehavior>() {
            public boolean filter(UserBehavior userBehavior) throws Exception {
                if ("pv".equals(userBehavior.getBehavior())) {
                    return true;
                }
                return false;
            }
        })
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new ItemViewWindowResult());

        DataStream<String> result = aggStream
                .keyBy("windowEnd")
                .process(new MyKeyedProcessFunction(3));

        dataStream.print("data");
        aggStream.print("agg");
        result.print("result");

        env.execute("streaming word count");

    }


    static class CountAgg implements AggregateFunction<UserBehavior,Long,Long> {

        public Long createAccumulator() {
            return 0L;
        }

        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        public Long getResult(Long aLong) {
            return aLong;
        }

        public Long merge(Long aLong, Long acc1) {
            return acc1 + aLong;
        }
    }

    static class ItemViewWindowResult implements WindowFunction<Long,ItemViewCount, Tuple, TimeWindow> {

        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            long itemId = ((Tuple1<Long>) tuple).f0;
            long ts = timeWindow.getEnd();
            long count = iterable.iterator().next();
            collector.collect(new ItemViewCount(itemId, ts, count));
        }
    }

    static class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple, ItemViewCount, String>{

        private ListState<ItemViewCount> listState;

        private int topN = 5;

        private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        public MyKeyedProcessFunction(int _topN) {
            this.topN = _topN;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("itemViewCount-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            listState.add(itemViewCount);
            // 注意：注册一个 windowEnd + 1之后的触发器
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //super.onTimer(timestamp, ctx, out);
            List<ItemViewCount> tmpList = new ArrayList<ItemViewCount>();
            Iterator<ItemViewCount> iter = listState.get().iterator();
            while (iter.hasNext()) {
                tmpList.add(iter.next());
            }
            listState.clear();
            Collections.sort(tmpList, new Comparator<ItemViewCount>() {
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int)(o2.getCount() - o1.getCount());
                }
            });

            StringBuilder sb = new StringBuilder();
            sb.append("[窗口结束时间：" + df.format(new Date(timestamp - 1)) + "]");
            for (int i = 0; i < Math.min(this.topN,tmpList.size()); i++) {
                ItemViewCount itemViewCount = tmpList.get(i);
                sb.append("\n==>id:" + itemViewCount.getItemId() + "\tcount:" +itemViewCount.getCount());
            }
            out.collect(sb.toString());

        }
    }
}
