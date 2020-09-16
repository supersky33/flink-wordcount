import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class WordCountKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.108:9092");
        properties.setProperty("group.id", "consumer-group");  // 可不设置
        FlinkKafkaConsumer<String> consumer011 =  new FlinkKafkaConsumer<String>(
                "coypan",
                new SimpleStringSchema(),
                properties);
        DataStreamSource<String> source = env.addSource(consumer011);
        source.print();
        env.execute("streaming word count");

    }


}
