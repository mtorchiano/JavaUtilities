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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The class provides the methods to read the rows of a CSV (Comma Separated Values)
 * file from a file or from a URL (caching the contents in a local file).
 * 
 * The rows can be read in two different ways:
 * <ul>
 * <li> using the column names provided in the header row (the first row), in this case
 *      each row will be encoded as a {@code Map<String,String>} mapping the column name to the row element.
 *      See: {@link #openNamedStream(String filePath)}, {@link #openNamedStream(InputStream input)},
 *      and {@link #openNamedStreamUrl(String url)}.
 *      <p>
 *      Example:
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
 *      Prints the unique values found in the {@code Anno} column.
 *      
 * <li> using a positional encoding, in this case each row will be returned as a
 *      {@code List<String>} where each row element is stored in its relative position.
 *      See: {@link #openStream(InputStream filePath)}, {@link #openStream(InputStream input)},
 *      and {@link #openNamedStream(String url)}.
 *      <p>
 *      Example:
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
 *      Prints the sorted unique values found in the second column.
 *      
 * </ul>
 * 
 * <b>Warning</b>: this class has been designed so that each thread uses a separate instance, created though {@link #newInstance()}.
 * 
 * @author Marco Torchiano
 * @version 1.0
 *
 */
public class CsvParser {
	
	private char separator = ','; // the element separator character used in the CSV file
	private boolean headerLine = true; // indicates the first line in the file is a header line
	
	private CsvParser(){}  // this class is meant to be instantiated through at factory method.
	private CsvParser(char separator){ // this class is meant to be instantiated through at factory method.
		this.separator = separator;
	}  
	
	/**
	 * Factory method used to instantiate a new CsvParser object
	 * 
	 * @return a new CsvParser object
	 */
	public static CsvParser newInstance(){
		return new CsvParser();
	}

	/**
	 * Factory method used to instantiate a new CsvParser object
	 * 
	 * @param separator the separator character to be used by this parser
	 * @return a new CsvParser object
	 */
	public static CsvParser newInstance(char separator){
		return new CsvParser(separator);
	}

	/**
	 * Retrieves the current separator character. The default character is ','.
	 * @return separator character
	 */
	public char getSeparator(){return separator;}
	/**
	 * Set the new separator character.
	 * It can be used to parse CSV files using different separators, e.g. ';'
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
	 * Returns a stream of rows from a CSV file, the elements are named
	 * according to the column names defined in the header row.
	 * 
	 * @param filePath the path of the local CSV file
	 * @return a stream of {@code Map}s each corresponding to a line
	 * @throws IOException in case of IO errors
	 */
	public Stream<Map<String,String>> openNamedStream(String filePath) throws IOException {
		return openNamedStream(new FileInputStream(filePath));
	}

	/**
	 * Returns a stream of rows from a CSV url, the elements are named
	 * according to the column names defined in the header row.
	 * 
	 * @param url the URL of a resource with CSV content
	 * @return a stream of {@code Map}s each corresponding to a line
	 * @throws IOException in case of IO errors
	 */
	public Stream<Map<String,String>> openNamedStreamUrl(String url) throws IOException{
		return openNamedStream(cachedUrl(url));
	}

	/**
	 * Returns a stream of rows from an input stream containing a CSV content, the elements are named
	 * according to the column names defined in the header row.
	 * 
	 * @param input the input stream linked to a CSV content
	 * @return a stream of {@code Map}s each corresponding to a line
	 * @throws IOException in case of IO errors
	 */
	public Stream<Map<String,String>> openNamedStream(InputStream input) throws IOException{
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String headerLine = reader.readLine();
		if(headerLine.indexOf(separator)==-1){
			throw new IOException("The header line does not contain any separator character ('" + separator + "')");
		}
		final List<String>headers = parseCsvLine(headerLine);
		return reader.lines().map(
				line -> parseCsvLine(line).stream()
						//.map(String::trim)
						.collect(HashMap::new,
								(rowMap,element) -> rowMap.put(headers.get(rowMap.size()),element),
								(rm1,rm2) -> rm1.putAll(rm2)
						)
				)
				;
	}
	
	/**
	 * Returns a stream of rows from a CSV file, the elements are 
	 * access by position.
	 * 
	 * If the {@code headerLine} is {@code true} the first line is skipped
	 * as it should not contain actual data. 
	 * 
	 * @param filePath the path of the local CSV file
	 * @return a stream of {@code List}s each corresponding to a line
	 * @throws IOException in case of IO errors
	 */
	public Stream<List<String>> openStream(String filePath) throws IOException{
		return openStream(new FileInputStream(filePath));
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
	public Stream<List<String>> openStreamUrl(String url) throws IOException{
		return openStream(cachedUrl(url));
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
	public Stream<List<String>> openStream(InputStream input) throws IOException{
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		if(headerLine) reader.readLine(); // skip first line containing the headers.
		return reader.lines().map(
					line -> parseCsvLine(line)
				)
				;
	}

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
}
