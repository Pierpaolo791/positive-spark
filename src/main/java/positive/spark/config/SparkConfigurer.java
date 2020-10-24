package positive.spark.config;


import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;


public class SparkConfigurer {
	
	public static SparkConf getSparkConf() {
		SparkConf sparkConf = new SparkConf().setAppName("Positive Spark").setMaster("local[2]");
		sparkConf.set("es.index.auto.create", "true");
		sparkConf.set("es.nodes", "10.0.100.51");
		sparkConf.set("es.resource", "tap/positive");
		sparkConf.set("es.input.json", "yes");
		return sparkConf;
	}
	
	public static Map<String, Object> getKafkaStreamingConfig() {
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", "10.0.100.25:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "positive-spark-streaming");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		return kafkaParams;
	}
	
	public static Map<String, Object> getKafkaStreamingProducerConfig() {
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", "10.0.100.25:9092");
		kafkaParams.put("key.serializer", StringSerializer.class);
		kafkaParams.put("value.serializer", StringSerializer.class);
		return kafkaParams;
	}

}
