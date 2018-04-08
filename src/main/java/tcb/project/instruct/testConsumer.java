package tcb.project.instruct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class testConsumer {
	public static void main(String[] args) throws IOException {
		//Number of consumers
		
		
		ExecutorService executor = Executors.newFixedThreadPool(1);
		consumer cons = new consumer();

		executor.submit(cons);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				
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
