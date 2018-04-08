package tcb.project.instruct;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class structTextTwo implements fileStructInterface{
	
	private long NUMERO;
	private String IDENTIFIANT;
	private String CODE_MESSAGE;
	private String DATE_ACTION_COMMERCIALE;
	private String CIVILITE;
	private String NOM;
	public long getNUMERO() {
		return NUMERO;
	}
	public void setNUMERO(long nUMERO) {
		NUMERO = nUMERO;
	}
	public String getIDENTIFIANT() {
		return IDENTIFIANT;
	}
	public void setIDENTIFIANT(String iDENTIFIANT) {
		IDENTIFIANT = iDENTIFIANT;
	}
	public String getCODE_MESSAGE() {
		return CODE_MESSAGE;
	}
	public void setCODE_MESSAGE(String cODE_MESSAGE) {
		CODE_MESSAGE = cODE_MESSAGE;
	}
	public String getDATE_ACTION_COMMERCIALE() {
		return DATE_ACTION_COMMERCIALE;
	}
	public void setDATE_ACTION_COMMERCIALE(String dATE_ACTION_COMMERCIALE) {
		DATE_ACTION_COMMERCIALE = dATE_ACTION_COMMERCIALE;
	}
	public String getCIVILITE() {
		return CIVILITE;
	}
	public void setCIVILITE(String cIVILITE) {
		CIVILITE = cIVILITE;
	}
	public String getNOM() {
		return NOM;
	}
	public void setNOM(String nOM) {
		NOM = nOM;
	}
	
	@Override
	public void writeHbase(String line,Connection connection) {
		  try (Admin admin = connection.getAdmin()) {

	            if (!admin.tableExists(TableName.valueOf("structtable3"))){

	                HTableDescriptor tableDescriptor = new
	                        HTableDescriptor(TableName.valueOf("structtable3"));

	                tableDescriptor.addFamily(new HColumnDescriptor("StructOne"));
	                tableDescriptor.addFamily(new HColumnDescriptor("StructTwo"));
	                tableDescriptor.addFamily(new HColumnDescriptor("StructThree"));

	                admin.createTable(tableDescriptor);
	                System.out.println(" Table created ");
	            }
	            
	            String[] values = line.split(";", -1);
	            
	            Table table = connection.getTable(TableName.valueOf("structtable3"));
	            
	            Put p = new Put(Bytes.toBytes(values[0])); 

	            p.addColumn(Bytes.toBytes("StructTwo"),Bytes.toBytes("NUMERO"),Bytes.toBytes(values[0]));
	            p.addColumn(Bytes.toBytes("StructTwo"),Bytes.toBytes("IDENTIFIANT"),Bytes.toBytes(values[1]));
	            p.addColumn(Bytes.toBytes("StructTwo"),Bytes.toBytes("CODE_MESSAGE"),Bytes.toBytes(values[2]));
	            p.addColumn(Bytes.toBytes("StructTwo"),Bytes.toBytes("DATE_ACTION_COMMERCIALE"),Bytes.toBytes(values[3]));
	            p.addColumn(Bytes.toBytes("StructTwo"),Bytes.toBytes("CIVILITE"),Bytes.toBytes(values[4]));
	            p.addColumn(Bytes.toBytes("StructTwo"),Bytes.toBytes("NOM"),Bytes.toBytes(values[5]));
	            
	            table.put(p);
	            table.close();
	            
	        } catch (IOException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
	        
	    
		System.out.println("Struct text two ");
		
		
	}
	
	@Override
	public void writeES(String line,RestHighLevelClient client) {
		
		String[] values = line.split(";", -1);
		
		IndexRequest request = new IndexRequest(
                "struct", 
                "StructTwo",  
                values[0]);
		
		  //Using Map
        Map<String, Object> jsonMap = new HashMap<String, Object>();
        
        jsonMap.put("NUMERO",values[0]);
        jsonMap.put("IDENTIFIANT",values[1]);
        jsonMap.put("CODE_MESSAGE",values[2]);
        jsonMap.put("DATE_ACTION_COMMERCIALE",values[3]);
        jsonMap.put("CIVILITE",values[4]);
        jsonMap.put("NOM",values[5]);

        //create the source
        request.source(jsonMap, XContentType.JSON);

        try {
			IndexResponse indexResponse = client.index(request);
			System.out.println(indexResponse.getType());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	

}
