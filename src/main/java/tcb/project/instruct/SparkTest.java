package tcb.project.instruct;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class SparkTest implements Runnable {

	@Override
	public void run() {

		SparkConf sparkConf = new SparkConf().setAppName("SparkConsumer").setMaster("local[*]");
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "malek-pc:6667");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "g1");
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", true);

		Collection<String> topics = Arrays.asList("structproducer5");

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jsc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		System.out.println("reading");
		// stream.foreachRDD(rdd -> {
		//
		// OffsetRange[] offsetRanges = ((HasOffsetRanges)
		// rdd.rdd()).offsetRanges();
		// rdd.foreachPartition(consumerRecords -> {
		// System.out.println(consumerRecords.toString());
		// OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
		// System.out.println(o.topic() + " " + o.partition() + " " +
		// o.fromOffset() + " " + o.untilOffset());
		// });
		// });

		JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String, String>, String>() {

			public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
				System.out.println(kafkaRecord.value());
				return kafkaRecord.value();
			}
		});
		lines.print();
		jsc.start();
		try {
			jsc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// Pattern regexp1 = Pattern.compile("(\\w)+\\;+(\\d)+\\,(\\d)+\\,+");
		// Pattern regexp2 = Pattern.compile("(\\w)+\\;+(\\w)+\\|+(\\w)+\\|+");
		// Pattern regexp3 =
		// Pattern.compile("(\\w)+;+;+(\\w)+\\_+(\\w)+\\_+(\\w)+;+(\\w)");
		//
		// Matcher matcherOne = regexp1.matcher("");
		// Matcher matcherTwo = regexp2.matcher("");
		// Matcher matcherThree = regexp3.matcher("");
		//

		// abstructFactory structFactory = FactoryProducer.getFactory();
		// Hbase config
		// Configuration conf = HBaseConfiguration.create();
		// conf.set("hbase.master", "malek-pc:16000");
		/// Set the configuration: force the configuration
		// conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		// Instantiating HbaseAdmin class
		// Connection conn;
		// Instantiating ES
		// RestClient restClient = RestClient.builder(
		// new HttpHost("localhost", 9200, "http")).build();
		//
		// RestHighLevelClient client =
		// new RestHighLevelClient(restClient);

	}

}
