package tcb.project.instruct;

import org.apache.hadoop.hbase.client.Connection;
import org.elasticsearch.client.RestHighLevelClient;

public interface fileStructInterface {

	void writeHbase(String line,Connection connection);
	void writeES(String line,RestHighLevelClient client);

}
