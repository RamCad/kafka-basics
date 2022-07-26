package poc.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {

  private static final Logger log = LoggerFactory.getLogger(Producer.class);

  public static void main(String[] args) {
    log.info("main");
    // create producer properties
    Properties properties = new Properties();
    /**
     * keys can be hardcoded or can be used from ProducerConfig.class
     */
//    properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the producer
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

    // create a producer record
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "kafka message 1");

    // send data - asynchronous
    kafkaProducer.send(producerRecord);

    // flush and close the producer - synchronous
    kafkaProducer.flush();
    kafkaProducer.close();
  }
}
