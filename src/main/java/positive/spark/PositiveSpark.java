package positive.spark;

import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.sound.sampled.LineListener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
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
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.vader.sentiment.analyzer.SentimentAnalyzer;

import positive.spark.config.SparkConfigurer;
import scala.Tuple2;

public class PositiveSpark implements Serializable {

	private static final long serialVersionUID = 1L;

	private SparkProxy spark;
	private transient JavaStreamingContext streamingContext;

	public PositiveSpark() {
		spark = SparkProxy.getInstance();
		streamingContext = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.getSparkContext()),
				Durations.seconds(10));
		startStreamProcessing();

	}

	private void startStreamProcessing() {
		System.out.println("Start stream processing...");
		getMessageStream().mapToPair(record -> new Tuple2<>(record.key(), record.value())).map(tuple2 -> tuple2._2)
			.foreachRDD(rdd ->predictEstimatedTimeThenSendToES(rdd));
		
		streamingContext.start();
		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	private JavaInputDStream<ConsumerRecord<String, String>> getMessageStream() {
		System.out.println("Call getMessageStream()...");
		Map<String, Object> kafkaParams = SparkConfigurer.getKafkaStreamingConfig();
		Collection<String> topics = Arrays.asList("tap");
		JavaInputDStream<ConsumerRecord<String, String>> messageStream = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		System.out.println("PRINT message stream");
		System.out.println("Connect to " + kafkaParams.get("bootstrap.servers"));
		return messageStream;
	}

	private void predictEstimatedTimeThenSendToES(JavaRDD<String> rdd) {
		Dataset<Row> dataset = spark.convertJsonRDDtoDataset(rdd);
		
		if (!dataset.isEmpty()) {
			//dataset.show(); 
			/*dataset = dataset
					.map((MapFunction<Row, Row>) row -> row, 
							RowEncoder.apply(new StructType(new StructField[] {
									new StructField("platform", DataTypes.StringType, true, Metadata.empty()),
									new StructField("userId", DataTypes.StringType, true, Metadata.empty()),
									new StructField("message", DataTypes.StringType, true, Metadata.empty()),
									new StructField("groupId", DataTypes.StringType, true, Metadata.empty()),
								 })));*/
			dataset = dataset.withColumn("timestamp", lit(current_timestamp().cast(DataTypes.TimestampType)));
			
			SentimentAnalyzer sentimentAnalyzer = null;
			try {
				System.out.println((String)dataset.collectAsList().get(0).getAs("polarPositive"));
				sentimentAnalyzer = new SentimentAnalyzer((String)dataset.collectAsList().get(0).getAs("polarPositive"));
				sentimentAnalyzer.analyze();
			} catch (IOException e) {
				e.printStackTrace();
			} 
			
			dataset = dataset.withColumn("polarPositive", lit(sentimentAnalyzer.getPolarity().get("positive")).cast(DataTypes.FloatType));
			
			
			dataset.show();
			RelationalGroupedDataset datasetGroupingByUser = dataset.groupBy(dataset.col("userId"));
			Dataset<Row> datasetGroupingByUserIdPositive = datasetGroupingByUser.sum("polarPositive");
			//Dataset<Row> datasetGroupingByUserIdNegative = datasetGroupingByUser.sum("polarNegative");
			datasetGroupingByUserIdPositive.show();
			//dataset.collectAsList().forEach( x -> System.out.println((String)x.getAs("message")));
			//SentimentAnalyzer helloWorld = new Sentiment()
			JavaEsSpark.saveJsonToEs(dataset.toJSON().toJavaRDD(), "tap/positive");
		}
	}

}
