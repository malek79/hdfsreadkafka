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

public class structTextOne implements fileStructInterface {
	
	private long NUMERO;
	private String IDENTIFIANT;
	private String CODE_MESSAGE;
	private float MONTANT_TTC;
	private String DATE_ECHEANCE_FACTURE;
	private String DATE_ECHEANCE_APUREMENT;
	private String MARQUEUR4;
	private String MARQUEUR5;
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
	public float getMONTANT_TTC() {
		return MONTANT_TTC;
	}
	public void setMONTANT_TTC(float mONTANT_TTC) {
		MONTANT_TTC = mONTANT_TTC;
	}
	public String getDATE_ECHEANCE_FACTURE() {
		return DATE_ECHEANCE_FACTURE;
	}
	public void setDATE_ECHEANCE_FACTURE(String dATE_ECHEANCE_FACTURE) {
		DATE_ECHEANCE_FACTURE = dATE_ECHEANCE_FACTURE;
	}
	public String getDATE_ECHEANCE_APUREMENT() {
		return DATE_ECHEANCE_APUREMENT;
	}
	public void setDATE_ECHEANCE_APUREMENT(String dATE_ECHEANCE_APUREMENT) {
		DATE_ECHEANCE_APUREMENT = dATE_ECHEANCE_APUREMENT;
	}
	public String getMARQUEUR4() {
		return MARQUEUR4;
	}
	public void setMARQUEUR4(String mARQUEUR4) {
		MARQUEUR4 = mARQUEUR4;
	}
	public String getMARQUEUR5() {
		return MARQUEUR5;
	}
	public void setMARQUEUR5(String mARQUEUR5) {
		MARQUEUR5 = mARQUEUR5;
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

	            p.addColumn(Bytes.toBytes("StructOne"),Bytes.toBytes("NUMERO"),Bytes.toBytes(values[0]));
	            p.addColumn(Bytes.toBytes("StructOne"),Bytes.toBytes("IDENTIFIANT"),Bytes.toBytes(values[1]));
	            p.addColumn(Bytes.toBytes("StructOne"),Bytes.toBytes("CODE_MESSAGE"),Bytes.toBytes(values[2]));
	            p.addColumn(Bytes.toBytes("StructOne"),Bytes.toBytes("MONTANT_TTC"),Bytes.toBytes(values[3]));
	            p.addColumn(Bytes.toBytes("StructOne"),Bytes.toBytes("DATE_ECHEANCE_FACTURE"),Bytes.toBytes(values[4]));
	            p.addColumn(Bytes.toBytes("StructOne"),Bytes.toBytes("DATE_ECHEANCE_APUREMENT"),Bytes.toBytes(values[5]));
	            p.addColumn(Bytes.toBytes("StructOne"),Bytes.toBytes("MARQUEUR4"),Bytes.toBytes(values[6]));
	            p.addColumn(Bytes.toBytes("StructOne"),Bytes.toBytes("MARQUEUR5"),Bytes.toBytes(values[7]));
	            
	            table.put(p);
	            table.close();
	            
	        } catch (IOException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
	        
	    
		System.out.println("Struct text one ");
		
		
	}
	
	@Override
	public void writeES(String line,RestHighLevelClient client) {
		
		String[] values = line.split(";", -1);
		
		IndexRequest request = new IndexRequest(
                "struct", 
                "StructOne",  
                values[0]);
			
		  //Using Map
        Map<String, Object> jsonMap = new HashMap<String, Object>();
        
        jsonMap.put("NUMERO",values[0]);
        jsonMap.put("IDENTIFIANT",values[1]);
        jsonMap.put("CODE_MESSAGE",values[2]);
        jsonMap.put("MONTANT_TTC",values[3]);
        jsonMap.put("DATE_ECHEANCE_FACTURE",values[4]);
        jsonMap.put("DATE_ECHEANCE_APUREMENT",values[5]);
        jsonMap.put("MARQUEUR4",values[6]);
        jsonMap.put("MARQUEUR5",values[7]);

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
