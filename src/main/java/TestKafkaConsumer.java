import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TestKafkaConsumer {
  public static void main(String[] args) {

    Properties consumerConfig = new Properties();
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.10.1.94:9092");
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    consumerConfig.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig);
    LogConsumerListener rebalanceListener = new LogConsumerListener();
    consumer.subscribe(Collections.singletonList("test-topic-res"), rebalanceListener);

    while (true) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(10000));
      System.out.println("receive " + records.count());
      for (ConsumerRecord<byte[], byte[]> record : records) {
        System.out.printf(
            "Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n",
            record.topic(), record.partition(), record.offset(), record.key(), record.value());
      }
      consumer.commitSync();
    }
  }
}
