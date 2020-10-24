package positive.spark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import positive.spark.config.SparkConfigurer;

public class MyKafkaUtils {

	public static void sendMessageToKafka(String json) {
		KafkaProducer<String, String>kafkaProducer = new  KafkaProducer<>(SparkConfigurer.getKafkaStreamingProducerConfig());
		kafkaProducer.send(new
				 ProducerRecord<String,String>("telegram-action",json));
	}
	
}
