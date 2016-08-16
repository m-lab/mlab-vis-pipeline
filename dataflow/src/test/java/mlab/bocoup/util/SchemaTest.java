package mlab.bocoup.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import org.junit.Test;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import mlab.bocoup.util.Schema;

public class SchemaTest {
	
	TableSchema ts;
	
	private final String[] expectedNames = {"a_string", "a_number", "a_float"};
	private final String[] types = {"STRING", "INTEGER", "FLOAT"};
	private final String[] modes = {"REQUIRED", "REQUIRED", "NULLABLE"};
	
	@Test
	public void testFromJSONFile() throws IOException {
		ts = Schema.fromJSONFile("src/test/resources/SchemaTest.json");
		assertFalse(ts.isEmpty());
		
		List<TableFieldSchema> s = ts.getFields();
		assertEquals(s.size(), 3);
		
		for(int i = 0; i < s.size(); i++) {
			TableFieldSchema tfs = s.get(i);
			assertEquals(tfs.getName(), expectedNames[i]);
			assertEquals(tfs.getType(), types[i]);
			assertEquals(tfs.getMode(), modes[i]);
		}
	}

}
