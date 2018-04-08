package tcb.readHdfs;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class writeHbase {

	public static void main(String[] args) throws IOException {

		AtomicBoolean closed = new AtomicBoolean();

		String uri = "hdfs://malek-pc:8020";
		URI hdfsuri = URI.create(uri);

		String path = "/user/project1";

		Configuration conf = new Configuration();

		// Set FileSystem URI
		conf.set("fs.defaultFS", uri);
		// Because of Maven
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		// Set HADOOP user
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		System.setProperty("hadoop.home.dir", "/");
		// Get the filesystem - HDFS
		FileSystem fs = FileSystem.get(hdfsuri, conf);

		FileStatus[] filestatus = fs.listStatus(new Path(path));
		Path[] paths = FileUtil.stat2Paths(filestatus);

		Properties kafkaProps = new Properties();
		kafkaProps.put("group.id", "g18");
		kafkaProps.put("bootstrap.servers", "malek-pc:6667");
		kafkaProps.put("auto.commit.interval.ms", "30000");
		kafkaProps.put("auto.offset.reset", "earliest");
		kafkaProps.put("session.timeout.ms", "30000");
		kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProps);
		for (Path pathchild : paths) {
			closed.set(true);
			System.out.println("***********" + pathchild.getName() + "***********");

			consumer.subscribe(Arrays.asList(pathchild.getName()));

			abstructFactory structFactory = FactoryProducer.getFactory();

			while (closed.get()) {

				ConsumerRecords<String, String> records = consumer.poll(1000);
				if (records.isEmpty()) {

					closed.set(false);
				}
				if (closed.get()) {

					for (ConsumerRecord<String, String> record : records) {

						// get an object of Shape Circle

						if ((int) record.value().split(";").length == 8) {

							fileStructInterface structtext1 = structFactory.getStructure("8");
							
							System.out.println(record.value().split(";").length);
							System.out.println(record.value());
						}

						// storeRecordInDB(record);
						// record.value().split("\\t");
					}
				}
			}

		}
		consumer.close();
	}

}
