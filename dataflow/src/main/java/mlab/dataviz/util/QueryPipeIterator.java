package mlab.dataviz.util;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

import mlab.dataviz.query.QueryBuilder;

public class QueryPipeIterator implements Iterator<PCollection<TableRow>> {

	private static final Logger LOG = LoggerFactory.getLogger(QueryPipeIterator.class);

	public static int NO_BATCH = 0;
	public static int BATCH_BY_DATE = 1;
	public static int BATCH_BY_ROW_INDEX = 2;

	private int batchType;
	private Pipeline pipe;
	private String queryString;
	private String queryName;

	// for date batching:
	private String queryFile;
	private Object[] queryParams;

	private String startDate;
	private String finalDate;
	private int numOfDays;
	ArrayList<DateRange> dateRanges;
	Iterator<DateRange> dateRangeIterator;

	// for row_idx batching
	private int incrementBy;
	private long startingRowIndex;
	private long lastRowIndex;

	/**
	 * No batching.
	 * @param pipe
	 * @param queryString
	 */
	public QueryPipeIterator(Pipeline pipe, String queryString) {
		this.pipe = pipe;

		this.batchType = NO_BATCH;
		this.queryString = queryString;
		this.queryName = queryString;
	}

	/**
	 * Batching by dates.
	 * @param pipe  The pipeline on which to run
	 * @param queryFile  The query file
	 * @param queryParams  The parameters to substitute in the query
	 * @param startDate  The start date
	 * @param finalDate  The last date
	 * @param numOfDays  The number of days between dates that the date range will be broken into
	 * @throws ParseException if dates cannot be parsed.
	 */
	public QueryPipeIterator(Pipeline pipe, String queryFile, Object[] queryParams, String startDate,
			String finalDate, int numOfDays) throws ParseException {

		this.pipe = pipe;

		this.batchType = BATCH_BY_DATE;
		this.queryFile = queryFile;
		this.startDate = startDate;
		this.finalDate = finalDate;
		this.numOfDays = numOfDays;

		dateRanges = TimeChunk.byNumberOfDays(this.startDate, this.finalDate, this.numOfDays);
		dateRangeIterator = dateRanges.iterator();
	}

	/**
	 * Batching by row index. This assumes there is a row index available.
	 * Assuming the query has a "Where ROW_ID < {0}" entry and a "LIMIT {1}" entry.
	 * @param pipe  Pipeline against which to run
	 * @param queryFile  The query file to run
	 * @param queryParams  The parameters to substitute within the query
	 * @param incrementBy  the number of rows to increment by.
	 */
	public QueryPipeIterator(Pipeline pipe, String queryFile, Object[] queryParams, long startingRowIndex, int incrementBy) {

		this.pipe = pipe;

		this.batchType = BATCH_BY_ROW_INDEX;
		this.startingRowIndex = startingRowIndex;
		this.incrementBy = incrementBy;
		this.lastRowIndex = -1;
	}

	/**
	 * Checks to see if there is another set of rows.
	 * @returns boolean
	 */
	public boolean hasNext() {
		if (this.batchType == NO_BATCH) {

			// if we are not batching, then there is only one run to be had here.
			return false;
		} else if (this.batchType == BATCH_BY_DATE) {
			return dateRangeIterator.hasNext();

		} else if (this.batchType == BATCH_BY_ROW_INDEX) {
			// TODO: figure out how to handle this for batching by row ids...
		}

		return false;
	}

	/**
	 * Returns the next set of rows.
	 * @returns PCollection of table rows.
	 */
	public PCollection<TableRow> next() {

		// get query
		if (this.batchType == BATCH_BY_DATE) {

			DateRange nextRange = dateRangeIterator.next();
			this.queryName = "Date range: " +
					nextRange.getDisplayStartTimestampStr() + " - " +
					nextRange.getDisplayEndTimestampStr();

			LOG.info(this.queryName);

			Object[] params = {
					nextRange.getDisplayStartTimestampStr(),
					nextRange.getDisplayEndTimestampStr()
			};

			// build parameters
			QueryBuilder qb;
			try {
				qb = new QueryBuilder(this.queryFile, params);
				this.queryString = qb.getQuery();
				LOG.debug("Query: \n" + this.queryString);
			} catch (IOException e) {
				LOG.error(e.getMessage());
				LOG.error(e.getStackTrace().toString());
			}
		}

		PCollection<TableRow> rows = this.pipe.apply("Running query " + queryName, BigQueryIO.readTableRows().fromQuery(queryString).usingStandardSql());

		return rows;
	}
}
