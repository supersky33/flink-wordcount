import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class MySource {

    public static void main(String[] args) throws Exception {

        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.addSource(new SourceFunction<String>() {
            public void run(SourceContext<String> sourceContext) throws Exception {
                Random random = new Random();
                while (true) {
                    int nextInt = random.nextInt(100);
                    sourceContext.collect("random:" + nextInt);
                    Thread.sleep(1000);
                }
            }

            public void cancel() {

            }
        });

        source.print();

        env.execute("streaming word count");

    }

}
