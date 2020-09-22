import domain.SensorReading;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class WaterMark {

    public static void main(String[] args) throws Exception {
        //定义socket的端口号
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置事件时间语义
        // 在DataStream调用assignTimestampAndWatermarks 传入BoundedOutOfOrdernessTimestampExtractor
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //连接socket获取输入的数据
        DataStreamSource<String> inputStream = env.socketTextStream
                ("localhost", 6677, "\n");

        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            public SensorReading map(String s) throws Exception {
                String[] arr = s.split(",");
                if (arr.length == 3) {
                    return new SensorReading(arr[0], Long.parseLong(arr[1]), Double.parseDouble(arr[2]));
                }
                else {
                    return new SensorReading("error", 0, 0);
                }
            }
        }).assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(SensorReading sensorReading) {
                        return sensorReading.ts * 1000L;
                    }
                });

        // 15s统计一次，窗口内各传感器所有温度任最小值，以及最新的时间戳
        DataStream<SensorReading> resultStream = dataStream
                .keyBy("id")
                .timeWindow(Time.seconds(10))
                //.allowedLateness(Time.minutes(1))
                //.sideOutputLateData(new OutputTag<SensorReading>("late"))
                .reduce(new ReduceFunction<SensorReading>() {
                    public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                        return new SensorReading(sensorReading.id, t1.ts, Math.min(sensorReading.tp, t1.tp));
                    }
                });
        dataStream.print("dataStream");
        resultStream.print("output");
        env.execute("start job");

    }


}
