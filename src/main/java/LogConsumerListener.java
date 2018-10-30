import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class LogConsumerListener implements ConsumerRebalanceListener {
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Called onPartitionsRevoked with partitions:" + partitions);
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Called onPartitionsAssigned with partitions:" + partitions);
    }
}