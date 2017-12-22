package mlab.dataviz.util;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.util.HashMap;

/**
 * Wraps a pipeline configuration object that specifies the relevant
 * tables and query files for each type of pipeline run. Example
 * file looks like this:
 *
 *  "downloads_ip_by_day_base": {
        "queryFile": "./data/bigquery/queries/base_downloads_ip_by_day.sql",
        "schemaFile": "./data/bigquery/schemas/base_downloads_ip.json",
        "outputTable": "data_viz.base_downloads_ip_by_day",
        "startDateFromTable": "data_viz.base_downloads_ip_by_day",
        "ndtTable": "`measurement-lab.public_v3_1.ndt_all`",
        "mergeTable": "data_viz.base_merged_ip_by_day",
        "mergeTableSchema": "./data/bigquery/schemas/all_ip.json",
        "withISPTable": "data_viz.all_ip_by_day",
        "withISPTableSchema": "./data/bigquery/schemas/all_ip.json"
    },
 * @author iros
 */
public class PipelineConfig {

	private static final String QUERY_FILE = "queryFile";
	private static final String SCHEMA_FILE = "schemaFile";
	private static final String OUTPUT_TABLE = "outputTable";
	private static final String START_DATE_FROM_TABLE = "startDateFromTable";
	private static final String NDT_TABLE = "ndtTable";
	private static final String MERGE_TABLE = "mergeTable";
	private static final String MERGE_TABLE_SCHEMA = "mergeTableSchema";
	private static final String WITH_ISP_TABLE = "withISPTable"; // @todo rename this
	private static final String WITH_ISP_TABLE_SCHEMA = "withISPTableSchema";

	// full config
	private JSONObject config = null;

	// the specific values for the type of config object.
	private HashMap<String, String> values;

	private static final Logger LOG = LoggerFactory.getLogger(PipelineConfig.class);

	/**
	 * @constructor
	 * Creates a new config from the json file, and picks a single object from it
	 * that matches the specific run in question.
	 * @param filename Config file path
	 * @param configName Config property object name
	 */
	@SuppressWarnings("unchecked")
	public PipelineConfig(String filename, String configName) {
		JSONParser jp = new JSONParser();
		try {
			this.config = (JSONObject) jp.parse(new FileReader(filename));
			this.switchConfigs(configName);
		} catch (Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * @constructor
	 * Creates a new config, but does not select a property from it to read.
	 * A switch call is required.
	 * @param filename Config file path
	 */
	public PipelineConfig(String filename) {
		JSONParser jp = new JSONParser();
		try {
			this.config = (JSONObject) jp.parse(new FileReader(filename));
		} catch (Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
		}
	}

	public void switchConfigs(String configName) throws Exception {
		this.values = (HashMap<String, String>) this.config.get(configName);
		if (this.values == null) {
			throw new Exception("No such config property name " + configName);
		}
	}

	public String getQueryFile() {
		return this.values.get(QUERY_FILE);
	}

	public String getSchemaFile() {
		return this.values.get(SCHEMA_FILE);
	}

	public String getOutputTable() {
		return this.values.get(OUTPUT_TABLE);
	}

	public String getStartDateFromTable() {
		return this.values.get(START_DATE_FROM_TABLE);
	}

	public String getNDTTable() {
		// Check if we have an env variable set for this. If we do, use that
		// instead.
		String ndtFromEnv = System.getenv("NDT_TABLE");
		if (ndtFromEnv != null && ndtFromEnv.length() > 0) {
			LOG.info("Using NDT_TABLE from environment");
			return ndtFromEnv;
		} else {
			LOG.info("Using NDT_TABLE from pipeline configuration file");
			return this.values.get(NDT_TABLE);
		}
	}

	public String getMergeTable() {
		return this.values.get(MERGE_TABLE);
	}

	public String getMergeTableSchema() {
		return this.values.get(MERGE_TABLE_SCHEMA);
	}

	public String getWithISPTable() {
		return this.values.get(WITH_ISP_TABLE);
	}

	public String getWithISPTableSchema() {
		return this.values.get(WITH_ISP_TABLE_SCHEMA);
	}
}
