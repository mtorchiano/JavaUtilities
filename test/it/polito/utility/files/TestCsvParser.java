package it.polito.utility.files;

import java.io.IOException;
import java.util.Collection;

import junit.framework.TestCase;
import static java.util.stream.Collectors.*;

public class TestCsvParser extends TestCase {
	String urlSemicolon = "http://www.dati.piemonte.it/catalogodati/scarica.html?idallegato=252";
	String urlColon = "http://softeng.polito.it/courses/05CBI/Open%20Data%20-%20Scuole%20Piemontesi%20.csv";

	public void testUrlSemicolon() throws IOException{
		//String url = "http://www.dati.piemonte.it/catalogodati/scarica.html?idallegato=703";  // 7z file...
		//String url = "http://softeng.polito.it/courses/05CBI/Open%20Data%20-%20Scuole%20Piemontesi%20.csv";


		CsvParser parser = CsvParser.newInstance(';');
		parser.setDetectSeparator(false);

		Collection<String> province =
		parser.openNamedRowsUrl(urlSemicolon)
		.map( row -> row.get("Provincia") )
		.distinct()
		.collect(toList());
		;
		
		System.out.println(province);
		assertTrue(province.contains("ASTI"));
		assertTrue(province.contains("TORINO"));
	}

	public void testUrlComma() throws IOException{
		//String url = "http://www.dati.piemonte.it/catalogodati/scarica.html?idallegato=703";  // 7z file...
		//String url = "http://softeng.polito.it/courses/05CBI/Open%20Data%20-%20Scuole%20Piemontesi%20.csv";


		CsvParser parser = CsvParser.newInstance();
		parser.setDetectSeparator(false);
		Collection<String> province =
		parser.openNamedRowsUrl(urlColon)
		.map( row -> row.get("Provincia") )
		.distinct()
		.collect(toList());
		;
		
		System.out.println(province);
		assertTrue(province.contains("ASTI"));
		assertTrue(province.contains("TORINO"));
	}


	public void testUrlDetect() throws IOException{
		//String url = "http://www.dati.piemonte.it/catalogodati/scarica.html?idallegato=703";  // 7z file...
		//String url = "http://softeng.polito.it/courses/05CBI/Open%20Data%20-%20Scuole%20Piemontesi%20.csv";


		CsvParser parser = CsvParser.newInstance();
		parser.setSeparator(',');
		parser.setDetectSeparator(true);
		
		Collection<String> province =
		parser.openNamedRowsUrl(urlSemicolon)
		.map( row -> row.get("Provincia") )
		.distinct()
		.collect(toList());
		;
		
		System.out.println(province);
		assertTrue(province.contains("ASTI"));
		assertTrue(province.contains("TORINO"));
	}
	
	
	public void testDetectSeparator(){
		
		
		String line = "AA,BB,CC";
		
		CsvParser parser = CsvParser.newInstance();

		assertTrue(parser.guessSeparator(line));
		
		assertEquals(',',parser.getSeparator());

		 line = "AA;BB;CC";
		
		 parser = CsvParser.newInstance();

		assertTrue(parser.guessSeparator(line));
		
		assertEquals(';',parser.getSeparator());

	}
}
