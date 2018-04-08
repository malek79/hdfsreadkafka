package tcb.spark.test;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;

import scala.Tuple2;

public class SparkTextFile {

	public static void main(String[] args) throws InterruptedException {
		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example").master("local[*]")
				.getOrCreate();

		

        List<Integer> data = Arrays.asList(10, 11, 12, 13, 14, 15);
        Dataset<Integer> ds = spark.createDataset(data, Encoders.INT());

        System.out.println("*** only one column, and it always has the same name");
        ds.printSchema();

        ds.show();
        
  //      ds.filter(value -> value > 12 ).show();
        
        ds.filter(col("value").gt(12)).show();
        
		// Read from file JAVARDD
//		JavaRDD<String> textFile = spark.read().textFile("hdfs://127.0.1.1:8020/user/spark/input.txt").javaRDD();
//
//		JavaPairRDD<String, Integer> counts = textFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
//				.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
//
//		// System.out.println(counts.collect());
//
//		// Read from file Dataset
//		Dataset<String> reader = spark.read().textFile("hdfs://127.0.1.1:8020/user/spark/input.txt");
//
//		Dataset<String> splitter = reader.flatMap(s -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING());

		// splitter.groupBy("value").count().show();

		// Text search
		// Creates a DataFrame having a single column named "line"
//		JavaRDD<Row> rowRDD = textFile.map(RowFactory::create);
//
//		List<StructField> fields = Arrays.asList(DataTypes.createStructField("line", DataTypes.StringType, true));
//
//		StructType schema = DataTypes.createStructType(fields);
//
//		Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema);
//
//		Dataset<Row> errors = df.filter(col("line").like("%as%"));

		// Counts all the errors
//		System.out.println(errors.count());
//
//		// Counts errors mentioning MySQL
//		errors.filter(col("line").like("%MySQL%")).count();
//		// Fetches the MySQL errors as an array of strings
//		errors.filter(col("line").like("%MySQL%")).collect();
	}

}
