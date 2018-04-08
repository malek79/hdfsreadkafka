package tcb.readHdfs;

public class structGlobalFactory extends abstructFactory {

	@Override
	fileStructInterface getStructure(String struct) {

		if (struct == null) {
			return null;
		}

		if (struct.equalsIgnoreCase("19")) {
			return new structcsv();

		} else if (struct.equalsIgnoreCase("8")) {
			return new structTextOne();

		} else if (struct.equalsIgnoreCase("6")) {
			return new structTextTwo();
		}

		return null;

	}

}
