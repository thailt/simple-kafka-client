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
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS_CONFIG);
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-test");
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfig.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    consumerConfig.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig);
    LogConsumerListener rebalanceListener = new LogConsumerListener();
    consumer.subscribe(Collections.singletonList(KafkaConstants.TEST_TOPIC), rebalanceListener);

    while (true) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
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
