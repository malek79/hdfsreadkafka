package tcb.project.instruct;

public class structGlobalFactory extends abstructFactory {

	@Override
	fileStructInterface getStructure(String struct) {

		if (struct == null) {
			return null;
		}

		if (struct.equalsIgnoreCase("csv")) {
			return new structcsv();

		} else if (struct.equalsIgnoreCase("text1")) {
			return new structTextOne();

		} else if (struct.equalsIgnoreCase("text2")) {
			return new structTextTwo();
		}

		return null;

	}

}
