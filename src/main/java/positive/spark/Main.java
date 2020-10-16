package positive.spark;

import com.vader.sentiment.analyzer.SentimentAnalyzer;

public class Main {
	
	public static void main(String[] args) throws Exception {
		//new PositiveSpark();	
		SentimentAnalyzer sentiment = new SentimentAnalyzer("Good day");
		sentiment.analyze();
		System.out.println(sentiment.getPolarity());
	}

}
