package mlab.bocoup.util;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import mlab.bocoup.ExtractUpdateRowsPipeline;

public class Schema {
	
	private static final Logger LOG = LoggerFactory.getLogger(Schema.class);
	
	/**
	 * Returns a TableSchame from a json file. The file must be an array
	 * with objects in it containing the name and type of the column. Order
	 * matters.
	 * @param filepath  The path to the json file.
	 * @return TableSchema.
	 */
	public static TableSchema fromJSONFile(String filepath) {
		FileReader reader;
		JSONArray schemaArray;
		JSONParser jsonParser;
		
		TableSchema schema = null;
		  
		try {
			schema = new TableSchema();
			
			reader = new FileReader(filepath);
			jsonParser = new JSONParser();
			schemaArray = (JSONArray) jsonParser.parse(reader);
			
			Iterator<JSONObject> i = schemaArray.iterator();
			
			List<TableFieldSchema> fields = new ArrayList<>();
			
			while (i.hasNext()) {
				JSONObject schemaItem = (JSONObject) i.next();
				String fieldName = (String) schemaItem.get("name");
				String fieldType = (String) schemaItem.get("type");
				fields.add(new TableFieldSchema().setName(fieldName).setType(fieldType));
			}
			
			schema.setFields(fields);
		} catch (FileNotFoundException e) {
			LOG.error(e.getMessage());
		} catch (IOException | ParseException e) {
			LOG.error(e.getMessage());
		}
		
		return schema;
	}
}