package positive.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import positive.spark.config.SparkConfigurer;
import scala.Tuple2;

public class PositiveSpark implements Serializable {

	private static final long serialVersionUID = 1L;

	private SparkProxy spark;
	private transient JavaStreamingContext streamingContext;

	public PositiveSpark() {
		spark = SparkProxy.getInstance();
		streamingContext = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.getSparkContext()),
				Durations.seconds(5));
		try {
			System.out.println("Start stream processing...");
			startStreamProcessing();
		} catch (InterruptedException e) {
			System.err.println("Error to startStreamProcessing()...");
			e.printStackTrace();
		}

	}

	private void startStreamProcessing() throws InterruptedException {
		System.out.println("Start stream processing...");
		getMessageStream().mapToPair(record -> new Tuple2<>(record.key(), record.value())).map(tuple2 -> tuple2._2)
				.foreachRDD(rdd -> System.out.println("Nuovo RDD"));
		streamingContext.start();
		streamingContext.awaitTermination();

	}

	private JavaInputDStream<ConsumerRecord<String, String>> getMessageStream() {
		System.out.println("Call getMessageStream()...");
		Map<String, Object> kafkaParams = SparkConfigurer.getKafkaStreamingConfig();
		Collection<String> topics = Arrays.asList("telegram-message","telegram-action");
		JavaInputDStream<ConsumerRecord<String, String>> messageStream = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		return messageStream;
	}

	private void predictEstimatedTimeThenSendToES(JavaRDD<String> rdd) {
		Dataset<Row> dataset = spark.convertJsonRDDtoDataset(rdd);
		System.out.println("call predictEstimatedTimeThenSendToEs");
		if (!dataset.isEmpty()) {
			dataset = dataset.drop("platform", "userId", "message", "groupId");
			dataset.show();
			dataset = dataset
					.map((MapFunction<Row, Row>) row -> row, 
							RowEncoder.apply(new StructType(new StructField[] {
									new StructField("platform", DataTypes.StringType, true, Metadata.empty()),
									new StructField("userId", DataTypes.StringType, true, Metadata.empty()),
									new StructField("message", DataTypes.StringType, true, Metadata.empty()),
									new StructField("groupId", DataTypes.DoubleType, true, Metadata.empty()),
								 })));
			
			System.out.println("Nuovo dataset: \n"+dataset.toString());
		}
	}

}
