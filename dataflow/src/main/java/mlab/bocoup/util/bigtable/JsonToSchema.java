package mlab.bocoup.util.bigtable;

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


public class JsonToSchema {
	
	
	public static BigtableConfig fromJSONFile(String filepath) {
		FileReader reader;
		JSONParser jsonParser;
		
		BigtableConfig schema = null;
		  
		try {
			schema = new BigtableConfig();
			
			reader = new FileReader(filepath);
			jsonParser = new JSONParser();
			JSONObject schemaObject = (JSONObject)jsonParser.parse(reader);
			
			JSONArray rowKeys = (JSONArray)schemaObject.get("rowKeys");
			
			Iterator<JSONObject> i = rowKeys.iterator();
			
			
			while (i.hasNext()) {
				JSONObject schemaItem = (JSONObject) i.next();
				 
				String name = (String) schemaItem.get("name");
				Integer size = (Integer) schemaItem.get("length");
				BigtableRowKeySchema rowKey = new BigtableRowKeySchema(name, size);
				
				schema.rowKeys.add(rowKey);
			}
			
			JSONArray columns = (JSONArray)schemaObject.get("columns");
			i = columns.iterator();
			while (i.hasNext()) {
				JSONObject schemaItem = (JSONObject) i.next();
				
				String name = (String) schemaItem.get("name");
				String type = (String) schemaItem.get("type");
				String family = (String) schemaItem.get("family");
				
				BigtableColumnSchema column = new BigtableColumnSchema(name, type, family);
				schema.columns.add(column);
			}
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException | ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return schema;
	}
}