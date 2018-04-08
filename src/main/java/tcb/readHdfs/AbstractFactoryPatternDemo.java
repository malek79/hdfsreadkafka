package tcb.readHdfs;

public class AbstractFactoryPatternDemo {

	public static void main(String[] args) {
		
		  //get shape factory
	      abstructFactory structFactory = FactoryProducer.getFactory();

	      //get an object of Shape Circle
	      fileStructInterface structtext1 = structFactory.getStructure("text1");

	      //get an object of Shape Rectangle
	      fileStructInterface structtext2 = structFactory.getStructure("text2");

	      
	      //get an object of Shape Square 
	      fileStructInterface structcsv = structFactory.getStructure("csv");


	    
	}

}
