package kafka;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        // bootstrap.servers kafka集群地址 host1:port1,host2:port2 ....
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // key.deserializer 消息key序列化方式
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // value.deserializer 消息体序列化方式
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 0 异步发送消息
        for (int i = 0; i < 10; i++) {
            String data = "async :" + i;
            // 发送消息
            producer.send(new ProducerRecord<>("demo-topic", data));
        }

        // 1 同步发送消息 调用get()阻塞返回结果
        for (int i = 0; i < 10; i++) {
            String data = "sync : " + i;
            try {
                // 发送消息
                Future<RecordMetadata> send = producer.send(new ProducerRecord<>("demo-topic", data));
                RecordMetadata recordMetadata = send.get();
                System.out.println(recordMetadata);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 2 异步发送消息 回调callback()
        for (int i = 0; i < 10; i++) {
            String data = "callback : " + i;
            // 发送消息
            producer.send(new ProducerRecord<>("demo-topic", data), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // 发送消息的回调
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println(metadata);
                    }
                }
            });
        }

        producer.close();
    }
}
