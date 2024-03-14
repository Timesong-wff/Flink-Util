package org.example;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 旧版本连接kafka
 * 使用方法addSource()
 */
public class KafkaUtil {
    //    static String BOOTSTRAP_SERVERS1 = "c5kafka1:6667";
    //static String BOOTSTRAP_SERVERS2 = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    static String BOOTSTRAP_SERVERS2 = "c6if051:6667,c6if052:6667,c6if053:6667,c6if054:6667,c6if055:6667,c6if056:6667,c6if057:6667,c6if058:6667,c6if059:6667,c6if060:6667," + "c6if061:6667,c6if062:6667,c6if063:6667,c6if064:6667,c6if065:6667,c6if066:6667,c6if067:6667,c6if068:6667,c6if069:6667,c6if070:6667," + "c6if071:6667,c6if072:6667,c6if073:6667,c6if074:6667,c6if075:6667,c6if076:6667,c6if077:6667,c6if078:6667,c6if079:6667,c6if080:6667";

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties prop = new Properties();
        //prop.setProperty("security.protocol", "SASL_PLAINTEXT");  // 安全协议认证
        //prop.setProperty("security.protocol", "GSSAPI");  // 安全协议认证
        //prop.setProperty("sasl.kerberos.service.name", "ocdp");   //认证
        prop.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS2);
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty("auto.offset.reset", "earliest");
//        prop.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,30 * 1000L +" ");
        //序列化器
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new KafkaDeserializationSchema<String>() {
            // 判定是流中的最后的一个元素
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }


            //反序列化字节信息，从上游接受数据,如果是null值，则不输出，非null值，转换成String输出
            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                if (record != null && record.value() != null) {
                    return new String(record.value());
                }
                return null;
            }

            //获取流中类型信息字符串。返回字符串
            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        }, prop);
        return kafkaConsumer;
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String sinkTopic) {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS2);
        //prop.setProperty("security.protocol", "SASL_PLAINTEXT"); // 安全协议认证
        //prop.setProperty("sasl.kerberos.service.name", "ocdp");  //认证

        KafkaSerializationSchema<String> serializationSchema = new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<>(
                        sinkTopic, // target topic
                        element.getBytes(StandardCharsets.UTF_8)); // record contents
            }
        };

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                sinkTopic,             // target topic
                serializationSchema,    // serialization schema
                prop,             // producer config
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
        return kafkaProducer;
    }
}
