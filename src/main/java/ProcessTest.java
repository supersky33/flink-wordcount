import domain.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 注意： new OutputTag<String>("low"){}
 */

public class ProcessTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("localhost", 6677, "\n");

        DataStream<SensorReading> dataStream = text
                .flatMap(new FlatMapFunction<String, SensorReading>() {
            public void flatMap(String value, Collector<SensorReading> out) throws Exception {
                String[] splits = value.split(",");
                if (splits.length == 3) {
                    out.collect(new SensorReading(
                            splits[0],
                            Long.parseLong(splits[1]),
                            Double.parseDouble(splits[2])));
                }
            }
        });


        SingleOutputStreamOperator<String> alertStream = dataStream
                .process(new MyProcessFunction(50.0));
                //.process(new MyKeyedProcessFunction(10 * 1000L){});

        alertStream.print("high");
        alertStream.getSideOutput(new OutputTag<String>("low"){}).print("low");

        env.execute("streaming word count ");

    }

    // 分流
    static class MyProcessFunction extends ProcessFunction<SensorReading, String> {

        private double threshold;

        public MyProcessFunction(double _threshold) {
            this.threshold = _threshold;
        }

        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            if (value.tp > this.threshold) {
                out.collect(value.toString());
            }
            else {
                ctx.output(new OutputTag<String>("low"){}, value.toString());
            }
        }
    }

    static class MyKeyedProcessFunction extends KeyedProcessFunction<String,SensorReading,String> {

        private ValueState<Double> lastTempState = null;
        private ValueState<Long> timerTsState = null;

        private long interval;

        public MyKeyedProcessFunction() {}

        public MyKeyedProcessFunction(long _interval) {
            this.interval = _interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTempState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("last-temp", Double.class));
            timerTsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {
            if (lastTempState != null) {
                if (lastTempState.value() > sensorReading.tp) {
                    context.timerService().deleteProcessingTimeTimer(timerTsState.value());
                    timerTsState = null;
                }
                else if (lastTempState.value() < sensorReading.tp && timerTsState == null) {
                    long ts = context.timerService().currentProcessingTime() + this.interval; // +10S
                    context.timerService().registerProcessingTimeTimer(ts);
                    timerTsState.update(ts);
                }
            }
            lastTempState.update(sensorReading.tp);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("传感器:" + ctx.getCurrentKey() + "连续10秒上升");
            timerTsState = null;
        }


    }

}



