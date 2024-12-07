import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class UserLogKafka2MySQLMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:10181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<UserLog> userLog = env.addSource(new FlinkKafkaConsumer<>(
                        "userlog",   
                        new SimpleStringSchema(),
                        props)).setParallelism(1)
                .map(string -> JSON.parseObject(string, UserLog.class)); //Fastjson 数据处理

        userLog.addSink(new UserLogSinkToMySQL()); //数据 sink 到 mysql

        env.execute("Flink Job UserLog Kafka to MySQL");
    }
}
