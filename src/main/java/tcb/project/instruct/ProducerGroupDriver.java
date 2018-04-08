package tcb.project.instruct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProducerGroupDriver {
	public static void main(String[] args) throws IOException, InterruptedException {
		//Number of consumers
		
		int numProducers = 2;
		
		ExecutorService executor = Executors.newFixedThreadPool(numProducers+1);
		//consumer cons = new consumer();
		//SparkTest spark = new SparkTest();
		SparkKafka spark = new SparkKafka();
		final List<ProducerGroupLoop> producers = new ArrayList<>();
		List<String> paths = new ArrayList<>();
		paths.add("/user/project1/text");
		paths.add("/user/project1/csv");
		executor.submit(spark);
		Thread.sleep(100000);
		for (int i = 0; i < numProducers; i++) {
			ProducerGroupLoop producer = new ProducerGroupLoop(i, paths.get(i));
			producers.add(producer);
			executor.submit(producer);
		}
		
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (ProducerGroupLoop producer : producers) {
					try {
						
						producer.shutdown();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				executor.shutdown();
				try {
					executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		});
		
		
	}
}
