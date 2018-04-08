package tcb.project.instruct;

import java.io.IOException;
import java.util.Arrays;
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
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class consumer implements Runnable {

	@Override
	public void run() {

		Properties kafkaProps = new Properties();
		//
		kafkaProps.put("group.id", "groupe-1");
		kafkaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
		kafkaProps.put("bootstrap.servers", "malek-pc:6667");
		kafkaProps.put("enable.auto.commit", "false");
		kafkaProps.put("auto.offset.reset", "earliest");
		kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		Pattern regexp1 = Pattern.compile("(\\w)+\\;+(\\d)+\\,(\\d)+\\,+");
		Pattern regexp2 = Pattern.compile("(\\w)+\\;+(\\w)+\\|+(\\w)+\\|+");
		Pattern regexp3 = Pattern.compile("(\\w)+;+;+(\\w)+\\_+(\\w)+\\_+(\\w)+;+(\\w)");

		Matcher matcherOne = regexp1.matcher("");
		Matcher matcherTwo = regexp2.matcher("");
		Matcher matcherThree = regexp3.matcher("");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProps);
		System.out.println("Start Consumer");
		consumer.subscribe(Arrays.asList("structproducer5"));

		abstructFactory structFactory = FactoryProducer.getFactory();
		//Hbase config
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "malek-pc:16000");
/// Set the configuration: force the configuration
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		// Instantiating HbaseAdmin class
		Connection conn;
		// Instantiating ES
		RestClient restClient = RestClient.builder(
		        new HttpHost("localhost", 9200, "http")).build();
		
		RestHighLevelClient client =
			    new RestHighLevelClient(restClient);
		
		try {
			conn = ConnectionFactory.createConnection(conf);
		//
			fileStructInterface struct1 = structFactory.getStructure("text1");
			fileStructInterface struct2 = structFactory.getStructure("text2");
			fileStructInterface structcsv = structFactory.getStructure("csv");
			
		while (true) {

			ConsumerRecords<String, String> records = consumer.poll(100);
			
			for (ConsumerRecord<String, String> record : records) {

				matcherOne.reset(record.value());
				matcherTwo.reset(record.value());
				matcherThree.reset(record.value());
				
				if (matcherOne.find()) {

					struct1.writeHbase(record.value(),conn);
					struct1.writeES(record.value(), client);
				}else if (matcherTwo.find()){
					struct2.writeHbase(record.value(), conn);
					struct2.writeES(record.value(), client);

				}else if (matcherThree.find()){
					structcsv.writeHbase(record.value(), conn);
					structcsv.writeES(record.value(), client);
				}else {
					System.out.println("No match found");
				}

			}

		}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
