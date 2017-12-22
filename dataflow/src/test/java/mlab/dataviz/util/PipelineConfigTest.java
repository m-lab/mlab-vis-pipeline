package mlab.dataviz.util;

import static org.junit.Assert.*;
import mlab.dataviz.util.PipelineConfig;

import org.junit.Test;

public class PipelineConfigTest {

	private String filename = "src/test/resources/pipeline_config_test.json";
	private String configPropName = "uploads_ip_by_hour_base";
	private String configPropName2 = "downloads_ip_by_hour_base";

	@Test
	public void testCreateConfigObject() {
		PipelineConfig pc = new PipelineConfig(filename, configPropName);
		assert(pc != null);
	}

	@Test
	public void testFetchingProperties() {
		PipelineConfig pc = new PipelineConfig(filename, configPropName);
		assertEquals(pc.getEndDateFromTable(), "`measurement-lab.public_v3_1.ndt_all`");
		assertEquals(pc.getWithISPTable(), "data_viz.all_ip_by_hour");
		assertEquals(pc.getWithISPTableSchema(), "./data/bigquery/schemas/all_ip.json");
	}

	@Test
	public void testSwitchConfig() throws Exception {
		PipelineConfig pc = new PipelineConfig(filename, configPropName);
		assertEquals(pc.getQueryFile(), "./data/bigquery/queries/base_uploads_ip_by_hour.sql");
		pc.switchConfigs(configPropName2);
		assertEquals(pc.getQueryFile(), "./data/bigquery/queries/base_downloads_ip_by_hour.sql");
	}

	@Test
	public void testMissingConfigSwitch() {
		PipelineConfig pc = new PipelineConfig(filename, configPropName);
		assertEquals(pc.getQueryFile(), "./data/bigquery/queries/base_uploads_ip_by_hour.sql");
		try {
			pc.switchConfigs("NOT A CONFIG PROP NAME");
			assertFalse("Should have thrown error on missing config prop name", true);
		} catch (Exception e) {
			assertTrue("Missing config detected", true);
		}
	}
}
