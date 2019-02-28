package mlab.dataviz.util.bigtable;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BigtableConfig implements java.io.Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(BigtableConfig.class);

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	public ArrayList<BigtableRowKeySchema> rowKeys;
	public ArrayList<BigtableColumnSchema> columns;

	public String bigQueryQuery;
	public String bigQueryTable;
	public String bigQueryQueryFile;

	public String bigtableTableName;

	public String getBigtableTable() {
		return bigtableTableName;
	}


	public String getBigQueryQuery() {
		return bigQueryQuery;
	}

	public String getBigQueryTable() {
		return bigQueryTable;
	}

	public String getBigQueryQueryFile() {
		return bigQueryQueryFile;
	}


	public BigtableConfig() {
		this.rowKeys = new ArrayList<BigtableRowKeySchema>();
		this.columns = new ArrayList<BigtableColumnSchema>();
	}

	@SuppressWarnings("unchecked")
	public static BigtableConfig fromJSONFile(String filepath) {
		FileReader reader;
		JSONParser jsonParser;

		BigtableConfig schema = null;

		try {
			schema = new BigtableConfig();

			reader = new FileReader(filepath);
			jsonParser = new JSONParser();
			JSONObject schemaObject = (JSONObject)jsonParser.parse(reader);

			schema.bigQueryQuery = (String) schemaObject.get("bigquery_query");
			schema.bigQueryTable = (String) schemaObject.get("bigquery_table");
			schema.bigQueryQueryFile = (String) schemaObject.get("bigquery_queryfile");

			schema.bigtableTableName = (String) schemaObject.get("bigtable_table_name");
			JSONArray rowKeys = (JSONArray)schemaObject.get("row_keys");

			Iterator<JSONObject> i = rowKeys.iterator();


			while (i.hasNext()) {
				JSONObject schemaItem = i.next();

				String name = (String) schemaItem.get("name");
				Integer size = ((Long) schemaItem.get("length")).intValue();
				BigtableRowKeySchema rowKey = new BigtableRowKeySchema(name, size);

				schema.rowKeys.add(rowKey);
			}

			JSONArray columns = (JSONArray)schemaObject.get("columns");
			i = columns.iterator();
			while (i.hasNext()) {
				JSONObject schemaItem = i.next();

				String name = (String) schemaItem.get("name");
				String type = (String) schemaItem.get("type");
				String family = (String) schemaItem.get("family");

				BigtableColumnSchema column = new BigtableColumnSchema(name, type, family);
				schema.columns.add(column);
			}


		} catch (FileNotFoundException e) {
			LOG.error(e.getMessage());
			LOG.error(e.getStackTrace().toString());
		} catch (IOException | ParseException e) {
			LOG.error(e.getMessage());
			LOG.error(e.getStackTrace().toString());
		}

		return schema;
	}


}