import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

class LogCompleteMessageCallback implements Callback {

    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            System.out.println("Error while producing message to topic :" + recordMetadata);
            e.printStackTrace();
        } else {
            String message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
            System.out.println(message);
        }
    }
}