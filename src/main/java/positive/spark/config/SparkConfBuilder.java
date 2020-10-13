package positive.spark.config;

import org.apache.spark.SparkConf;

public class SparkConfBuilder {
	
	private SparkConf sparkConf; 
	
	public SparkConfBuilder() {
		sparkConf = new SparkConf(); 
	}
	
	public SparkConfBuilder setAppName(String name)  {
		sparkConf = sparkConf.setAppName(name).setMaster("local[2]");
		return this; 
	}
		

}
