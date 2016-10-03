package mlab.bocoup.query;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mlab.bocoup.util.FileIO;

/**
 * Builds a query from a query file path and an array of parameters in order.
 * Parameters must be specified in the path as follows {0}, {1} ... {n}.
 * 
 * Parameters are not required, at which point, the contents of the file will
 * be returned as the query itself.
 * @author iros
 *
 */
public class QueryBuilder {
	
	private String queryFile;
	private String queryString;
	private Object[] queryParams;
	private static final Logger LOG = LoggerFactory.getLogger(QueryBuilder.class);
	
	/**
	 * @private
	 * Reads a query from a file, and replaces any arguments with the 
	 * query parameters passed in the constructor. Saves the query string.
	 * @throws IOException
	 */
	private void _init() throws IOException {
		String content = "";
		try {
			content = FileIO.readFile(this.queryFile);
			if (this.queryParams != null) {
				this.queryString = MessageFormat.format(content, (Object[])this.queryParams);
			} else {
				this.queryString = content;
			}
	
		} catch (FileNotFoundException e) {
			LOG.error(e.getMessage());
		}
	}
	
	/**
	 * Takes a query file and a set of parameters, and places those
	 * parameters in the query file in order. The parameters should be in numbered
	 * form like so: {0}, {1}. The replacement will happen in the order of the
	 * provided parameters array.
	 * @param queryfile  the path to the query file. 
	 * @param params  an array of parameter objects to replace.
	 * @throws IOException
	 */
	public QueryBuilder(String queryfile, Object[] params) throws IOException {
		this.queryFile = queryfile;
		this.queryParams = params;
		_init();
	}
	
	/**
	 * Takes a query file and reads it into the query string without any additional
	 * parameters.
	 * @param queryfile
	 * @throws IOException
	 */
	public QueryBuilder(String queryfile) throws IOException {
		this.queryFile = queryfile;
		_init();
	}
	
	/**
	 * Returns the query string
	 * @return String  the query with substituted params.
	 */
	public String getQuery() {
		return this.queryString;
	}
	
	/**
	 * Returns a string-ified version of parameters separated by a comma if
	 * parameters exist. Otherwise, return the queryString.
	 * @return String
	 */
	public String toString() {
		if (this.queryParams != null) {
			String[] output = new String[this.queryParams.length];
			for(int i = 0; i < this.queryParams.length; i++) {
				output[i] = this.queryParams[i].toString();
			}
		
			return String.join(", ", output);
		} else {
			return this.getQuery();
		}
	}
}
