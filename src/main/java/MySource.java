import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
        source.addSink(new RichSinkFunction<String>() {
            private PreparedStatement insert;
            private PreparedStatement update;
            private Connection conn;

            @Override
            public void close() throws Exception {
                super.close();
                insert.close();
                update.close();
                conn.close();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
                insert = conn.prepareStatement("INSERT INTO flink (value) VALUES (?)");
                update = conn.prepareCall("UPDATE flink SET value = ? WHERE id = ?");
            }

            public void invoke(String value, Context context) {
                try {
                    insert.setString(1, value);
                    insert.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        });


        env.execute("streaming word count");

    }

}

