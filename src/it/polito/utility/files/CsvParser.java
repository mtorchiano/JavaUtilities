package it.polito.utility.files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;
import static java.util.Comparator.*;

/**
 * The class provides the methods to read the rows of a CSV (Comma Separated Values)
 * file from a file or from a URL (caching the contents in a local file).
 * <p>
 * The sequence of rows can be returned in two ways:
 * <ul>
 * <li>as a {@link java.util.stream.Stream} using the {@code open..} methods or
 * <li>as a {@link java.util.List} using the {@code load} methods.
 * </ul>
 * The individual rows can be represented in two different ways:
 * <ul>
 * <li> <b>Named</b>: using the column names provided in the header row (the first row), in this case
 *      each row will be encoded as a {@code Map<String,String>} mapping the column name to the row element.
 *      This is the return type for all the {@code openNamed..} and {@code loadNamed} methods.
 * <li> <b>Positional</b>: using a positional encoding, in this case each row will be returned as a
 *      {@code List<String>} where each row element is stored in its relative position.
 *      This is the return type for all the methods without {@code Named} in their names.
 * </ul>
 * In addition the possible source of the CSV content can be:
 * <ul>
 * <li> a file whose path is provided as a String,
 * <li> a url provided as a string argument to the {@code ..Url()} methods, or
 * <li> an {@link java.io.InputStream}.
 * </ul>
 * <table style="border:1px solid;" cellpadding="5px" cellspacing="0" >
 * <caption>Summary of CSV reading methods</caption>
 * <tr><th><th><th class="colFirst" >Positional<th class="colLast">Named
 * <tr class="altColor"><th rowspan="3"  class="colFirst">{@code Stream}
 * 		<th>File<td>{@link #openRows(String filePath)}<td>{@link #openNamedRows(String filePath)}
 * <tr class="rowColor"><th>Url<td>{@link #openRowsUrl(String url)}<td>{@link #openNamedRowsUrl(String url)}
 * <tr class="altColor"><th>InputStream<td>{@link #openRows(InputStream input)}<td>{@link #openNamedRows(InputStream input)}
 * <tr><td>&nbsp;
 * <tr class="altColor"><th rowspan="3"  class="colFirst">{@code List}
 * 		<th>File<td>{@link #loadRows(String filePath)}<td>{@link #loadNamedRows(String filePath)}
 * <tr class="rowColor"><th>Url<td>{@link #loadRowsUrl(String url)}<td>{@link #loadNamedRowsUrl(String url)}
 * <tr class="altColor"><th>InputStream<td>{@link #loadRows(InputStream input)}<td>{@link #loadNamedRows(InputStream input)}
 * </table>
 *
 * <h3>Examples</h3>
 * 
 * Using the Stream versions, loading the CSV content from a url,
 * we can use the named version to print the unique values found 
 * in the {@code Anno} column as follows:
 *      <pre>
 *      {@code
 *      CsvParser parser = CvsParser.newInstance();
 *      parser.openNamedStreamUrl(url) 
 *      	  .map( row -> row.get("Anno") )
 *      	  .distinct()
 *      	  .forEach(System.out::println)
 *      	  ;
 *      }
 *      </pre>
 * Using the Stream versions, loading the CSV content from a url,
 * we can use the named version to the sorted unique values found in the second column 
 * as follows:
 *      <pre>
 *      {@code 
 *      CsvParser parser = CvsParser.newInstance();
 *      parser.openNamedStreamUrl(url) 
 *      		.map( row -> row.get(1) )
 *      		.distinct()
 *      		.sorted()
 *      		.forEach(System.out::println)
 *      		;
 *      }
 *      </pre>
 * <p>
 * <b>Warning</b>: this class has been designed so that each thread uses a separate instance, created though {@link #newInstance()}.
 * 
 * 
 * @author Marco Torchiano
 * @version 1.1
 *
 */
public class CsvParser {
	private final static Character[] separators = {',',';','\t',':'};
	
	private char separator = ','; // the element separator character used in the CSV file
	private boolean detectSeparator = true; // indicates whether the separator must be detected automatically
	private boolean headerLine = true; // indicates the first line in the file is a header line
	private boolean useCachedUrl = true; // indicates whether a local cache of the url should be used
	
	private CsvParser(Characteristics... properties){
		for(Characteristics c : properties){
			switch(c){
			case MANUAL_SEPARATOR: detectSeparator = false;
									break;
			case NO_HEADER: headerLine = false;
								break;
			case NO_URL_CACHE: useCachedUrl = false;
								break;
			}
		}
	}  // this class is meant to be instantiated through at factory method.
	private CsvParser(char separator, Characteristics... properties){ // this class is meant to be instantiated through at factory method.
		this(properties);
		detectSeparator = false;
		setSeparator( separator );
	}  
	/**
	 * 
	 * Characteristics indicating the properties of a {@link CvsParser}.
	 *
	 */
	public static enum Characteristics {
		/**
		 * Indicates the separator is set manually and not automatically detected by the parser on the basis of the header line
		 */
		MANUAL_SEPARATOR,
		/**
		 * Indicates the CVS file does not contain any header line
		 */
		NO_HEADER,
		/**
		 * Indicates that when opening a URL no local cache has to be created
		 */
		NO_URL_CACHE
	}
	
	/**
	 * Factory method to instantiate a new CsvParser object
	 * 
	 * @param properties additional (optional) properties for the new CvsParser
	 * 
	 * @return a new CsvParser object
	 */
	public static CsvParser newInstance(Characteristics... properties){
		return new CsvParser(properties);
	}

	/**
	 * Factory method to instantiate a new CsvParser object.
	 * 
	 * A predefined element separator will be used. 
	 * The automatic separator detection is disable in this case, 
	 * as if a {@link Characteristics#MANUAL_SEPARATOR} is passed among the properties.
	 * 
	 * @param properties additional (optional) properties for the new CvsParser
	 * @param separator the separator character to be used by this parser
	 * @return a new CsvParser object
	 */
	public static CsvParser newInstance(char separator,Characteristics... properties){
		return new CsvParser(separator,properties);
	}

	/**
	 * Retrieves the current separator character. The default character is ','.
	 * 
	 * @return separator character
	 */
	public char getSeparator(){return separator;}
	
	/**
	 * Set a new separator character.
	 * It can be used to parse CSV files using different separators, e.g. ';'
	 * 
	 * @param sep the new separator character
	 */
	public void setSeparator(char sep){ 
		separator=sep; 
		regexp = "(\"(([^\"]*|\"\")*)\"|([^\""+separator+"]*))("+separator+"|$)";
		p = Pattern.compile(regexp);
	}

	/**
	 * Retrieves the {@code headerLine} option.
	 * 
	 * When it is true the first line in the file is assumed to contain the header
	 * with the column names and not actual data 
	 * 
	 * @return the value of the option
	 */
	public boolean headerLine() {
		return headerLine;
	}
	
	/**
	 * Sets the {@code headerLine} option.
	 * 
	 * When it is true the first line in the file is assumed to contain the header
	 * with the column names and not actual data 
	 * 
	 * @param headerLine the new value for the {@code headerLine} option.
	 */
	public void setHeaderLine(boolean headerLine) {
		this.headerLine = headerLine;
	}
	
	/**
	 * Checks the {@code useCachedUrl} property.
	 * 
	 * When the property is true (default) opening a url
	 * actually stores a local cache copy (if not already present) 
	 * and opens it.
	 * 
	 * @return the current value of the property
	 */
	public boolean useCachedUrl() {
		return useCachedUrl;
	}
	
	/**
	 * Sets the {@code useCachedUrl} property.
	 * 
	 * When the property is true (default) opening a url
	 * actually stores a local cache copy (if not already present) 
	 * and opens it.
	 * 
	 * @param useCachedUrl the new value of the property
	 */
	public void setUseCachedUrl(boolean useCachedUrl) {
		this.useCachedUrl = useCachedUrl;
	}
	
	/**
	 * Check the value of the detectSeparator property.
	 * 
	 * When the property is true the parser will attempt detecting the 
	 * separator used in the file automatically.
	 * 
	 * @return the value of the property
	 */
	public boolean detectSeparator() {
		return detectSeparator;
	}
	
	/**
	 * Set the value of the {@code detectSeparator} property.
	 * 
	 * When the property is true the parser will attempt detecting the 
	 * separator used in the file automatically.
	 * 
	 * @param detectSeparator
	 */
	public void setDetectSeparator(boolean detectSeparator) {
		this.detectSeparator = detectSeparator;
	}
	
	/**
	 * Returns the stream of rows from a CSV file, the elements are named
	 * according to the column names defined in the header row.
	 * 
	 * @param filePath the path of the local CSV file
	 * @return a stream of {@code Map}s each corresponding to a line
	 * @throws IOException in case of IO errors
	 */
	public Stream<Map<String,String>> openNamedRows(String filePath) throws IOException {
		return openNamedRows(new FileInputStream(filePath));
	}

	/**
	 * Returns a stream of rows from a CSV url, the elements are named
	 * according to the column names defined in the header row.
	 * 
	 * @param url the URL of a resource with CSV content
	 * @return a stream of {@code Map}s each corresponding to a line
	 * @throws IOException in case of IO errors
	 */
	public Stream<Map<String,String>> openNamedRowsUrl(String url) throws IOException{
		return openNamedRows(cachedUrl(url));
	}

	/**
	 * Returns a stream of rows from an input stream containing a CSV content, the elements are named
	 * according to the column names defined in the header row.
	 * 
	 * @param input the input stream linked to a CSV content
	 * @return a stream of {@code Map}s each corresponding to a line
	 * @throws IOException in case of IO errors
	 */
	public Stream<Map<String,String>> openNamedRows(InputStream input) throws IOException{
		if(!headerLine){ // no header line is expected so names cannot be found.
			throw new IOException("Cannot return a named row stream when the headerLine property is set to true.");
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String firstLine = reader.readLine();
		if(detectSeparator){
			if(!guessSeparator(firstLine)){
				throw new IOException("Cannot detect separator.");
			}
		}else
		if(firstLine.indexOf(separator)==-1){
			throw new IOException("The header line does not contain any separator character ('" + separator + "')");
		}
		final List<String>headers = parseCsvLine(firstLine);
		return reader.lines().map(
				line -> parseCsvLine(line).stream()
						//.map(String::trim)
						.collect(LinkedHashMap::new,
								(rowMap,element) -> rowMap.put(headers.get(rowMap.size()),element),
								(rm1,rm2) -> rm1.putAll(rm2)
						)
				)
				;
	}
	
	/**
	 * Returns a stream of rows from a CSV file. 
	 * The elements inside the row are 
	 * access by position.
	 * 
	 * If the {@code headerLine} is {@code true} the first line is skipped
	 * as it does not contain actual data. 
	 * 
	 * @param filePath the path of the local CSV file
	 * @return a stream of {@code List}s each corresponding to a line
	 * @throws IOException in case of IO errors
	 */
	public Stream<List<String>> openRows(String filePath) throws IOException{
		return openRows(new FileInputStream(filePath));
	}

	
	/**
	 * Returns a stream of rows from a CSV url, the elements are 
	 * access by position.
	 * 
	 * If the {@code headerLine} is {@code true} the first line is skipped
	 * as it should not contain actual data.
	 * 
	 * @param url the URL of the resource with CSV content
	 * @return a stream of {@code List}s each corresponding to a line
	 * @throws IOException in case of IO errors
	 */
	public Stream<List<String>> openRowsUrl(String url) throws IOException{
		return openRows(cachedUrl(url));
	}

	/**
	 * Returns a stream of rows from an input stream containing a CSV content, the elements are
	 * access by position.
	 * 
	 * If the {@code headerLine} is {@code true} the first line is skipped
	 * as it should not contain actual data. 
	 * 
	 * @param input the input stream linked to a CSV content
	 * @return a stream of {@code List}s each corresponding to a line
	 * @throws IOException in case of IO errors
	 */
	public Stream<List<String>> openRows(InputStream input) throws IOException{
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		if(headerLine){
			String firstLine = reader.readLine(); // skip first line containing the headers.
			if(detectSeparator){
				if(!guessSeparator(firstLine)){
					throw new IOException("Cannot detect separator.");
				}
			}else
				if(firstLine.indexOf(separator)==-1){
					throw new IOException("The header line does not contain any separator character ('" + separator + "')");
				}
		}
		return reader.lines().map(
					line -> parseCsvLine(line)
				)
				;
	}
	
	
	/**
	 * Load the rows of a CSV file into a {@code List}.
	 * 
	 * The result is a list of rows, each row is stored in a list of strings.
	 * 
	 * @param filePath the path of the CSV file
	 * @return a {@code List} of {@code List<String>}s each representing a row
	 * @throws IOException in case of IO errors
	 */
	public List<List<String>> loadRows(String filePath) throws IOException{
		return loadRows(new FileInputStream(filePath));
	}

	/**
	 * Load the rows of a CSV resource found at the given URL into a {@code List}.
	 * 
	 * The result is a list of rows, each row is stored in a list of strings.
	 * 
	 * @param url the URL of the resource with CSV content
	 * @return a {@code List} of {@code List<String>}s each representing a row
	 * @throws IOException in case of IO errors
	 */
	public List<List<String>> loadRowsUrl(String url) throws IOException{
		return loadRows(cachedUrl(url));
	}

	/**
	 * Load the rows of a CSV content into a {@code List}.
	 * 
	 * The result is a list of rows, each row is stored in a list of strings.
	 * 
	 * @param input the input stream linked to a CSV content
	 * @return a {@code List} of {@code List<String>}s each representing a row
	 * @throws IOException in case of IO errors
	 */
	public List<List<String>> loadRows(InputStream input) throws IOException{
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		if(headerLine){
			String firstLine = reader.readLine(); // skip first line containing the headers.
			if(detectSeparator){
				if(!guessSeparator(firstLine)){
					throw new IOException("Cannot detect separator.");
				}
			}else
			if(firstLine.indexOf(separator)==-1){
				throw new IOException("The header line does not contain any separator character ('" + separator + "')");
			}
		}
		return reader.lines().map(
					line -> parseCsvLine(line)
				)
				.collect(toList())
				;
	}

	/**
	 * Load the rows of a CSV file into a {@code List}.
	 * 
	 * The result is a list of rows, each row is stored in a map having the column names from the heading as keys.
	 * 
	 * @param filePath the path of the CSV file
	 * @return a {@code List} of {@code Map<String,String>}s each representing a row
	 * @throws IOException in case of IO errors
	 */
	public List<List<String>> loadNamedRows(String filePath) throws IOException{
		return loadRows(new FileInputStream(filePath));
	}

	/**
	 * Load the rows of a CSV resource found at the given URL into a {@code List}.
	 * 
	 * The result is a list of rows, each row is stored in a map having the column names from the heading as keys.
	 * 
	 * @param url the URL of the resource with CSV content
	 * @return a {@code List} of {@code Map<String,String>}s each representing a row
	 * @throws IOException in case of IO errors
	 */
	public List<List<String>> loadNamedRowsUrl(String url) throws IOException{
		return loadRows(cachedUrl(url));
	}
	/**
	 * Load the rows of a CSV content into a {@code List}.
	 * 
	 * The result is a list of rows, each row is stored in a map having the column names from the heading as keys.
	 * 
	 * @param input the input stream linked to a CSV content
	 * @return a {@code List} of {@code Map<String,String>}s each representing a row
	 * @throws IOException in case of IO errors
	 */
	public List<Map<String,String>> loadNamedRows(InputStream input) throws IOException{
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String firstLine = reader.readLine();
		if(detectSeparator){
			if(!guessSeparator(firstLine)){
				throw new IOException("Cannot detect separator.");
			}
		}else
		if(firstLine.indexOf(separator)==-1){
			throw new IOException("The header line does not contain any separator character ('" + separator + "')");
		}
		final List<String>headers = parseCsvLine(firstLine);
		return reader.lines().map(
				line -> parseCsvLine(line).stream()
						.collect((Supplier<Map<String,String>>)LinkedHashMap::new,
								(rowMap,element) -> rowMap.put(headers.get(rowMap.size()),element),
								(rm1,rm2) -> rm1.putAll(rm2)
						)
				)
				.collect(toList())
				;
	}

	// ------------------------------- INTERNAL METHODS -----------------------------------------------
	// IO support methods
	/**
	 * General utility method to create a cache copy of a URL content
	 * and open an input stream from that local file.
	 * 
	 * @param url
	 * @return
	 * @throws IOException
	 */
	private static InputStream cachedUrl(String url) throws IOException{		
		String cacheName = "CSV" + url.hashCode() + ".cache";
		File cacheFile = new File(cacheName);
		
		if(! cacheFile.exists()){						
			URL theUrl = new URL(url);
			
			byte[] buffer = new byte[2048];
			InputStream in = theUrl.openStream();
			FileOutputStream out = new FileOutputStream(cacheFile);
			int n=0;
			while(true){
				n = in.read(buffer);
				if(n==-1) break;
				out.write(buffer, 0, n);
			}
			out.close();
			in.close();
		}
		return new FileInputStream(cacheFile);				
	}
	
	private String regexp = "(\"(([^\"]*|\"\")*)\"|([^\""+separator+"]*))("+separator+"|$)";
	private Pattern p = Pattern.compile(regexp);
	/**
	 * Parses a row from a CSV file and returns a list of strings
	 * 
	 * @param line
	 * @return the elements of the row
	 */
	private List<String> parseCsvLine(String line){
		ArrayList<String> elements = new ArrayList<>();		
		Matcher m = p.matcher(line);
		while(m.find()){
			if(m.group(2)!=null){
				elements.add(m.group(2).trim());
			}
			if(m.group(4)!=null){
				elements.add(m.group(1).trim());
			}
		}
		if(elements.get(elements.size()-1).equals("")){
			elements.remove(elements.size()-1);
		}
		return elements;
	}
	
    boolean guessSeparator(String line){
		Function<Character,Integer> countOccurrences = sep -> line.replaceAll("[^"+sep+"]+", "").length();

		Optional<Character> detected =
		Arrays.stream(separators)
		.collect(groupingBy(countOccurrences))
		.entrySet().stream()
//		.peek(System.out::println)
		.collect(maxBy(comparing(Map.Entry::getKey)))
//		.map( e -> { System.out.println("max:" + e); return e; })
		.filter( e -> e.getKey()>0 && e.getValue().size()==1 )
		.map( e -> e.getValue().get(0) )
		;

//		Optional<Character> detected =
//		Arrays.stream(separators)
//		.collect(groupingBy(countOccurrences))
//		.entrySet().stream()
//		.sorted(comparing(Map.Entry::getKey,reverseOrder())) // Note: comparing(Map.Entry::getKey).reversed() is not inferred corretly!!
//		.limit(1)
//		.filter( e -> e.getKey()>0 && e.getValue().size()==1 )
//		.map(e -> e.getValue().get(0))
//		.findFirst()
//		;
		
		detected.ifPresent( this::setSeparator );

		return detected.isPresent();
	}
}
