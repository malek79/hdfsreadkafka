package tcb.project.instruct;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class patternTest {

	public static void FirstPatternLineMatcher() {

		String msg1 = "0666696610;5002953777,00441,ECHEA_APURE;ECHEA_APURE_02;53,32;;20160201;;";

		String msg2 = "0676055211;SATHO_ACTCO|EE|SOU|20160126|TELEPHONE|PART_0018|DETECTION PROJETS RENO|ACCEPTEE|5001250339|RES_C34953|POLE PROJET;"
				+ "SATHO_ACTCO_001;;20160126;;Mme, M.; GUILLARD";
		
		String msg3 = "6011523436;;CRMMM6011523436CLOTU_DEMAN_0120160121;Mme;PRINGARBE;EDMONDE;;;;0640082566;"
				+ "CLOTU_DEMAN_01;Z031;CLOTU_DEMAN 21/01/2016;"
				+ "SIMM ETL;CLOTU_DEMAN_01 20160121;[MSG AUTO] CLOTUre de DEMANde courrier;NON;;;;;;;;;;";

		Pattern regexp1 = Pattern.compile("(\\w)+\\;+(\\d)+\\,(\\d)+\\,+");
		Pattern regexp2 = Pattern.compile("(\\w)+\\;+(\\w)+\\|+(\\w)+\\|+");
		Pattern regexp3 = Pattern.compile("(\\w)+;+;+(\\w)+\\_+(\\w)+\\_+(\\w)+;+(\\w)");


		Matcher matcherOne = regexp1.matcher("");
		Matcher matcherTwo = regexp2.matcher("");
		Matcher matcherThree = regexp3.matcher("");

		matcherOne.reset(msg3); // reset the input
		matcherTwo.reset(msg3); // reset the input
		matcherThree.reset(msg3); // reset the input


		// General
		if (matcherOne.find()) {
			System.out.println(msg3);
			System.out.println("Matcher One");
		} else {
			if (matcherTwo.find()) {
				System.out.println(msg3);
				System.out.println("Matcher Two");
			} else {
				if (matcherThree.find()) {
					System.out.println(msg3);
					System.out.println("Matcher Three");
				} else {
					throw new IllegalStateException("The line is bad");
				}
			}
		}

	}

	public static void main(String[] args) {

		FirstPatternLineMatcher();
	}

}
