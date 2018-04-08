package tcb.project.instruct;

import java.io.IOException;
import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class SparkTest2  {

	public static void main(String[] args) throws IOException {

		String bootstrapServers = "malek-pc:6667";
		   String subscribeType = "subscribe";
		   String topics = "structproducer5";
System.out.println("init");
		   SparkSession spark = SparkSession
		     .builder()
		     .master("local[2]")
		     .appName("JavaStructuredKafkaWordCount")
		     .getOrCreate();
System.out.println("reading");
		   // Create DataSet representing the stream of input lines from kafka
		   Dataset<String> lines = spark
		     .readStream()
		     .format("kafka")
		     .option("kafka.bootstrap.servers", bootstrapServers)
		     .option(subscribeType, topics)
		     .load()
		     .selectExpr("CAST(value AS STRING)")
		     .as(Encoders.STRING());
		   
		   // Generate running word count
		   Dataset<Row> wordCounts = lines.flatMap(
		       (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
		       Encoders.STRING()).groupBy("value").count();
System.out.println("tay");
		   // Start running the query that prints the running counts to the console
		   StreamingQuery query = wordCounts.writeStream()
		     .outputMode("complete")
		     .format("console")
		     .start();

		   try {
			   System.out.println("waiting");
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
