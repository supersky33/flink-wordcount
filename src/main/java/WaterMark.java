import com.oracle.webservices.internal.api.databinding.DatabindingMode;
import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WaterMark {

    public static void main(String[] args) throws Exception {
        //定义socket的端口号
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //连接socket获取输入的数据
        DataStreamSource<String> inputStream = env.socketTextStream
                ("localhost", 6677, "\n");
        inputStream.map(new MapFunction<String, Object>() {
            public Object map(String s) throws Exception {
                String [] arr = s.split(",");
                return new SensorReading(arr[0], Long.parseLong(arr[1]), Double.parseDouble(arr[2]));
            }
        });

        inputStream.print("output");

    }


}

@Data
class SensorReading {
    public String id;
    public long ts;
    public double tp;

    public SensorReading(String _id, long _ts, double _tp) {
        this.id = _id;
        this.ts = _ts;
        this.tp = _tp;
    }
}
