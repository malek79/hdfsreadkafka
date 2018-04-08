package tcb.project.instruct;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerGroupLoop implements Runnable {

	private static Properties props = new Properties();
	private static Producer<String, String> kafkaproducer;
	private final int id;
	private final String path;

	private static void configure(String servers) {

		props.put("bootstrap.servers", servers);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("bootstrap.servers", "malek-pc:6667");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaproducer = new KafkaProducer<String, String>(props);
	}

	public ProducerGroupLoop(int id, String path) throws IOException {
		this.id = id;
		this.path = path;
	}

	@Override
	public void run() {
		try {
			configure("malek-pc:6667");

			String uri = "hdfs://malek-pc:8020";
			URI hdfsuri = URI.create(uri);

			// ====== Init HDFS File System Object
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

			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			
			for (Path hdfsreadpath : paths) {

				CompressionCodec codec = factory.getCodec(hdfsreadpath);
				if (codec == null) {
					System.err.println("No codec found for " + path);
					System.exit(1);
				}
				InputStream in = null;
				String str = "";

				try {
					in = codec.createInputStream(fs.open(hdfsreadpath));

					InputStreamReader r = new InputStreamReader(in);

					BufferedReader br = new BufferedReader(r);

					while ((str = br.readLine()) != null) {
						kafkaproducer.send(new ProducerRecord<String, String>("structproducer5", str));
					}

				} finally {
					IOUtils.closeStream(in);
				}
			}
			System.out.println("Done");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void shutdown() throws IOException {

		kafkaproducer.close();
	}

}
