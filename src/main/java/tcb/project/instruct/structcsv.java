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

public class structcsv implements fileStructInterface {
	
	private long NUMERO_BP;
	private long NUMERO_PDL;
	private String ID_COMMUNICATION;
	private String CIVILITE;
	private String NOM_BP;
	private String PRENOM_BP;
	private String ADRESSE_MAIL;
	private long TEL_FIXE;
	private long TEL_BUREAU;
	private long TEL_PORTABLE;
	private String TYPE_MESSAGE;
	private String CANAL_DEFAUT;
	private String CODE_CAMPAGNE;
	private String CODE_INCA;
	private String CODE_ELEM_CAMPAGNE;
	private String DESI_ELEM_CAMPAGNE;
	private String TEMOIN;
	private String VALEUR_VARIABLE1;
	private String VALEUR_VARIABLE2;

	public long getNUMERO_BP() {
		return NUMERO_BP;
	}
	public void setNUMERO_BP(long nUMERO_BP) {
		NUMERO_BP = nUMERO_BP;
	}
	public long getNUMERO_PDL() {
		return NUMERO_PDL;
	}
	public void setNUMERO_PDL(long nUMERO_PDL) {
		NUMERO_PDL = nUMERO_PDL;
	}
	public String getID_COMMUNICATION() {
		return ID_COMMUNICATION;
	}
	public void setID_COMMUNICATION(String iD_COMMUNICATION) {
		ID_COMMUNICATION = iD_COMMUNICATION;
	}
	public String getCIVILITE() {
		return CIVILITE;
	}
	public void setCIVILITE(String cIVILITE) {
		CIVILITE = cIVILITE;
	}
	public String getNOM_BP() {
		return NOM_BP;
	}
	public void setNOM_BP(String nOM_BP) {
		NOM_BP = nOM_BP;
	}
	public String getPRENOM_BP() {
		return PRENOM_BP;
	}
	public void setPRENOM_BP(String pRENOM_BP) {
		PRENOM_BP = pRENOM_BP;
	}
	public String getADRESSE_MAIL() {
		return ADRESSE_MAIL;
	}
	public void setADRESSE_MAIL(String aDRESSE_MAIL) {
		ADRESSE_MAIL = aDRESSE_MAIL;
	}
	public long getTEL_FIXE() {
		return TEL_FIXE;
	}
	public void setTEL_FIXE(long tEL_FIXE) {
		TEL_FIXE = tEL_FIXE;
	}
	public long getTEL_BUREAU() {
		return TEL_BUREAU;
	}
	public void setTEL_BUREAU(long tEL_BUREAU) {
		TEL_BUREAU = tEL_BUREAU;
	}
	public long getTEL_PORTABLE() {
		return TEL_PORTABLE;
	}
	public void setTEL_PORTABLE(long tEL_PORTABLE) {
		TEL_PORTABLE = tEL_PORTABLE;
	}
	public String getTYPE_MESSAGE() {
		return TYPE_MESSAGE;
	}
	public void setTYPE_MESSAGE(String tYPE_MESSAGE) {
		TYPE_MESSAGE = tYPE_MESSAGE;
	}
	public String getCANAL_DEFAUT() {
		return CANAL_DEFAUT;
	}
	public void setCANAL_DEFAUT(String cANAL_DEFAUT) {
		CANAL_DEFAUT = cANAL_DEFAUT;
	}
	public String getCODE_CAMPAGNE() {
		return CODE_CAMPAGNE;
	}
	public void setCODE_CAMPAGNE(String cODE_CAMPAGNE) {
		CODE_CAMPAGNE = cODE_CAMPAGNE;
	}
	public String getCODE_INCA() {
		return CODE_INCA;
	}
	public void setCODE_INCA(String cODE_INCA) {
		CODE_INCA = cODE_INCA;
	}
	public String getCODE_ELEM_CAMPAGNE() {
		return CODE_ELEM_CAMPAGNE;
	}
	public void setCODE_ELEM_CAMPAGNE(String cODE_ELEM_CAMPAGNE) {
		CODE_ELEM_CAMPAGNE = cODE_ELEM_CAMPAGNE;
	}
	public String getDESI_ELEM_CAMPAGNE() {
		return DESI_ELEM_CAMPAGNE;
	}
	public void setDESI_ELEM_CAMPAGNE(String dESI_ELEM_CAMPAGNE) {
		DESI_ELEM_CAMPAGNE = dESI_ELEM_CAMPAGNE;
	}
	public String getTEMOIN() {
		return TEMOIN;
	}
	public void setTEMOIN(String tEMOIN) {
		TEMOIN = tEMOIN;
	}
	public String getVALEUR_VARIABLE1() {
		return VALEUR_VARIABLE1;
	}
	public void setVALEUR_VARIABLE1(String vALEUR_VARIABLE1) {
		VALEUR_VARIABLE1 = vALEUR_VARIABLE1;
	}
	public String getVALEUR_VARIABLE2() {
		return VALEUR_VARIABLE2;
	}
	public void setVALEUR_VARIABLE2(String vALEUR_VARIABLE2) {
		VALEUR_VARIABLE2 = vALEUR_VARIABLE2;
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

	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("NUMERO_BP"),Bytes.toBytes(values[0]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("NUMERO_PDL"),Bytes.toBytes(values[1]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("ID_COMMUNICATION"),Bytes.toBytes(values[2]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("CIVILITE"),Bytes.toBytes(values[3]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("NOM_BP"),Bytes.toBytes(values[4]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("PRENOM_BP"),Bytes.toBytes(values[5]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("ADRESSE_MAIL"),Bytes.toBytes(values[6]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("TEL_FIXE"),Bytes.toBytes(values[7]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("TEL_BUREAU"),Bytes.toBytes(values[8]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("TEL_PORTABLE"),Bytes.toBytes(values[9]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("TYPE_MESSAGE"),Bytes.toBytes(values[10]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("CANAL_DEFAUT"),Bytes.toBytes(values[11]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("CODE_CAMPAGNE"),Bytes.toBytes(values[12]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("CODE_INCA"),Bytes.toBytes(values[13]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("CODE_ELEM_CAMPAGNE"),Bytes.toBytes(values[14]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("DESI_ELEM_CAMPAGNE"),Bytes.toBytes(values[15]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("TEMOIN"),Bytes.toBytes(values[16]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("VALEUR_VARIABLE1"),Bytes.toBytes(values[17]));
	            p.addColumn(Bytes.toBytes("StructThree"),Bytes.toBytes("VALEUR_VARIABLE2"),Bytes.toBytes(values[18]));

	            table.put(p);
	            table.close();
	            
	        } catch (IOException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }
	        
	    
		System.out.println("Struct csv ");
		
		
	}
	@Override
	public void writeES(String line,RestHighLevelClient client) {
		
		String[] values = line.split(";", -1);
		
		IndexRequest request = new IndexRequest(
                "struct", 
                "StructThree",  
                values[0]);
		
		  //Using Map
        Map<String, Object> jsonMap = new HashMap<String, Object>();
        
        jsonMap.put("NUMERO_BP",values[0]);
        jsonMap.put("NUMERO_PDL",values[1]);
        jsonMap.put("ID_COMMUNICATION",values[2]);
        jsonMap.put("CIVILITE",values[3]);
        jsonMap.put("NOM_BP",values[4]);
        jsonMap.put("PRENOM_BP",values[5]);
        jsonMap.put("ADRESSE_MAIL",values[6]);
        jsonMap.put("TEL_FIXE",values[7]);
        jsonMap.put("TEL_BUREAU",values[8]);
        jsonMap.put("TEL_PORTABLE",values[9]);
        jsonMap.put("TYPE_MESSAGE",values[10]);
        jsonMap.put("CANAL_DEFAUT",values[11]);
        jsonMap.put("CODE_CAMPAGNE",values[12]);
        jsonMap.put("CODE_INCA",values[13]);
        jsonMap.put("CODE_ELEM_CAMPAGNE",values[14]);
        jsonMap.put("DESI_ELEM_CAMPAGNE",values[15]);
        jsonMap.put("TEMOIN",values[16]);
        jsonMap.put("VALEUR_VARIABLE1",values[17]);
        jsonMap.put("VALEUR_VARIABLE2",values[18]);

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
