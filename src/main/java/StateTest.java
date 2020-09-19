import domain.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTest {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream("localhost", 6677, "\n");

        //计算数据
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

        DataStream<String> alertStream = dataStream
                .keyBy("id")
                .flatMap(new MyAlertFlatMapFunction(10));

        dataStream.print("data-stream");
        alertStream.print("alert-stream");

        env.execute("streaming word count");

    }

}

class MyAlertFlatMapFunction extends RichFlatMapFunction<SensorReading, String> {

    private double threshold;
    private ValueState<Double> valueState;

    public MyAlertFlatMapFunction(double _threshold) {
        this.threshold = _threshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        valueState = getRuntimeContext()
                .getState(new ValueStateDescriptor<Double>("valueState", Double.class));
    }

    public void flatMap(SensorReading sensorReading, Collector<String> collector) throws Exception {
        if (valueState.value() != null) {
            double diff = Math.abs(sensorReading.tp - valueState.value());
            if (diff > this.threshold) {
                collector.collect(sensorReading.id + ":" + valueState.value() + "/" + sensorReading.tp);
            }
        }
        valueState.update(sensorReading.tp);
    }
}
