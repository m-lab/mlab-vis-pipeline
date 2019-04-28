package mlab.dataviz.util;
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

public class Schema {

	private static final Logger LOG = LoggerFactory.getLogger(Schema.class);

	private static JSONArray getJSONStruct(String filepath) throws IOException, ParseException {
		FileReader reader = new FileReader(filepath);
		JSONParser jsonParser = new JSONParser();
		return (JSONArray) jsonParser.parse(reader);
	}

	/**
	 * Returns a TableSchame from a json file. The file must be an array
	 * with objects in it containing the name and type of the column. Order
	 * matters.
	 * @param filepath  The path to the json file.
	 * @return TableSchema.
	 */
	@SuppressWarnings("unchecked")
	public static TableSchema fromJSONFile(String filepath) {
		JSONArray schemaArray;
		TableSchema schema = null;

		try {
			schema = new TableSchema();

			schemaArray = getJSONStruct(filepath);

			Iterator<JSONObject> i = schemaArray.iterator();

			List<TableFieldSchema> fields = new ArrayList<>();

			while (i.hasNext()) {
				JSONObject schemaItem = i.next();
				String fieldName = (String) schemaItem.get("name");
				String fieldType = (String) schemaItem.get("type");
				String fieldMode = (String) schemaItem.get("mode");
				fields.add(new TableFieldSchema()
						.setName(fieldName)
						.setType(fieldType)
						.setMode(fieldMode));
			}

			schema.setFields(fields);
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