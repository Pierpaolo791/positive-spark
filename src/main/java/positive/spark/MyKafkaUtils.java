package positive.spark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import positive.spark.config.SparkConfigurer;

public class MyKafkaUtils {
	
	private static KafkaProducer<String, String>kafkaProducer = new  KafkaProducer<>(SparkConfigurer.getKafkaStreamingProducerConfig());

	public static void sendMessageToKafka(String json) {
		
		kafkaProducer.send(new
				 ProducerRecord<String,String>("telegram-action",json));
	}
	
}
