package positive.spark;

import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vader.sentiment.analyzer.SentimentAnalyzer;

import positive.spark.config.SparkConfigurer;
import scala.Tuple2;

public class PositiveSpark implements Serializable {

	private static final long serialVersionUID = 1L;

	private SparkProxy spark;
	private transient JavaStreamingContext streamingContext;
	// private KafkaProducer<String, String> kafkaProducer;

	public PositiveSpark() {
		spark = SparkProxy.getInstance();
		streamingContext = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.getSparkContext()),
				Durations.seconds(15));
		// kafkaProducer = new
		// KafkaProducer<>(SparkConfigurer.getKafkaStreamingProducerConfig());
		startStreamProcessing();

	}

	private void startStreamProcessing() {
		System.out.println("Start stream processing...");
		getMessageStream().mapToPair(record -> new Tuple2<>(record.key(), record.value())).map(tuple2 -> tuple2._2)
				.foreachRDD(rdd -> makeSentimentAnalysisThenSendToEsAndKafka(rdd));

		streamingContext.start();
		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private JavaInputDStream<ConsumerRecord<String, String>> getMessageStream() {
		Map<String, Object> kafkaParams = SparkConfigurer.getKafkaStreamingConfig();
		Collection<String> topics = Arrays.asList("telegram-message");
		JavaInputDStream<ConsumerRecord<String, String>> messageStream = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		System.out.println("Connect to kafka server: " + kafkaParams.get("bootstrap.servers"));

		return messageStream;
	}

	private void makeSentimentAnalysisThenSendToEsAndKafka(JavaRDD<String> rdd) {
		Dataset<Row> dataset = spark.convertJsonRDDtoDataset(rdd);
		if (dataset.isEmpty())
			return;

		dataset = dataset.map((MapFunction<Row, Row>) row -> getRowWithPositivityAndNegativity(row),
				RowEncoder.apply(getStructTypeWithPositivityAndNegativity()));

		dataset = dataset.withColumn("timestamp", lit(current_timestamp().cast(DataTypes.TimestampType)));
		dataset.show();

		RelationalGroupedDataset datasetGroupingByUser = dataset.groupBy(dataset.col("userId"));
		Dataset<Row> datasetGroupingByUserIdPositive = datasetGroupingByUser.avg("positivity");
		Dataset<Row> datasetGroupingByUserIdNegative = datasetGroupingByUser.avg("negativity");

		Dataset<Row> avgNegativeAndPositive = datasetGroupingByUserIdNegative.join(datasetGroupingByUserIdPositive,
				"userId");
		avgNegativeAndPositive = avgNegativeAndPositive
				.join(dataset.drop("message", "timestamp", "positivity", "negativity"), "userId");
		avgNegativeAndPositive.show();

		avgNegativeAndPositive.foreach(x -> {
			if (((double) x.getAs("avg(positivity)")) < ((double) x.getAs("avg(negativity)"))) {
				System.out.println("User " + x.getAs("userId") + ": La negativita' supera la positivita'");
				sendBanAction(x);
			}
		});
		
		JavaEsSpark.saveJsonToEs(dataset.toJSON().toJavaRDD(), "tap/positive");

	}

	private void sendBanAction(Row row) {
		Message message = getMessageFromRow(row);
		ObjectMapper mapper = new ObjectMapper();
		String jsonMessage = "";
		try {
			jsonMessage = mapper.writeValueAsString(message);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		MyKafkaUtils.sendMessageToKafka(jsonMessage);
	}

	private Message getMessageFromRow(Row row) {
		Message message = new Message();
		message.setGroupId(row.getAs("groupId"));
		message.setPlatform(row.getAs("platform"));
		message.setUserId(row.getAs("userId"));
		message.setMessage("ban");
		return message;
	}

	private Row getRowWithPositivityAndNegativity(Row row) {
		float[] posAndNeg = getPositiveAndNegativeSentimentAnalysis(row.getAs("message"));
		return RowFactory.create(row.getAs("platform"), row.getAs("userId"), row.getAs("message"), row.getAs("groupId"),
				posAndNeg[0], posAndNeg[1]);
	}

	private StructType getStructTypeWithPositivityAndNegativity() {
		return new StructType(
				new StructField[] { new StructField("platform", DataTypes.StringType, true, Metadata.empty()),
						new StructField("userId", DataTypes.StringType, true, Metadata.empty()),
						new StructField("message", DataTypes.StringType, true, Metadata.empty()),
						new StructField("groupId", DataTypes.StringType, true, Metadata.empty()),
						new StructField("positivity", DataTypes.FloatType, true, Metadata.empty()),
						new StructField("negativity", DataTypes.FloatType, true, Metadata.empty()) });
	}

	private float[] getPositiveAndNegativeSentimentAnalysis(String text) {
		SentimentAnalyzer sentimentAnalyzer = null;
		try {
			sentimentAnalyzer = new SentimentAnalyzer(text);
		} catch (IOException e) {
			e.printStackTrace();
		}
		sentimentAnalyzer.analyze();

		return new float[] { sentimentAnalyzer.getPolarity().get("positive"),
				sentimentAnalyzer.getPolarity().get("negative") };
	}
}
