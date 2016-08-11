package mlab.bocoup;

import java.io.Serializable;
import java.util.Map;
import java.util.NavigableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import mlab.bocoup.dofn.AddLocalTimeFn;
import mlab.bocoup.util.PipelineOptionsSetup;
import mlab.bocoup.util.Schema;
import mlab.bocoup.dofn.ExtractZoneKeynameFn;
import mlab.bocoup.transform.CombineAsNavigableMapHex;
public class AddLocalTimePipeline {

	private static final Logger LOG = LoggerFactory.getLogger(AddLocalTimePipeline.class);
	private static final String INPUT_TABLE = "mlab-oti:bocoup.zz_base_by_day_with_isp_test"; //"mlab-oti:bocoup_prod.base_all_ip_by_day_with_isps";
	private static final String OUTPUT_TABLE = "mlab-oti:bocoup.zz_base_by_day_with_isp_test_localized"; // "mlab-oti:bocoup_prod.base_all_ip_by_day_with_isps_localized";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";
	
	// zones data
	private static String BQ_TIMEZONE_TABLE = "mlab-oti:bocoup.localtime_timezones";
	
	private BigQueryIO.Write.WriteDisposition writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
	private BigQueryIO.Write.CreateDisposition createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
	
	private Pipeline pipeline;
	private String inputTable = INPUT_TABLE;
	private String outputTable = OUTPUT_TABLE;
	private TableSchema outputSchema = Schema.fromJSONFile(OUTPUT_SCHEMA);
	private boolean writeData = false;

	public AddLocalTimePipeline(Pipeline p) {
		this.pipeline = p;
	}
	
	public PCollection<TableRow> loadData() {
		// read in the by IP data from `by_ip_day_base`
		PCollection<TableRow> utcOnlyTestRows = this.pipeline.apply(
				BigQueryIO.Read
				.named("Read " + this.inputTable)
				.from(this.inputTable));

		return utcOnlyTestRows;
	}
	
	public String getInputTable() {
		return inputTable;
	}

	public AddLocalTimePipeline setInputTable(String inputTable) {
		this.inputTable = inputTable;
		return this;
	}
	
	public String getOutputTable() {
		return outputTable;
	}

	public AddLocalTimePipeline setOutputTable(String outputTable) {
		this.outputTable = outputTable;
		return this;
	}
	
	public TableSchema getOutputSchema() {
		return outputSchema;
	}

	public AddLocalTimePipeline setOutputSchema(TableSchema outputSchema) {
		this.outputSchema = outputSchema;
		return this;
	}

	public BigQueryIO.Write.WriteDisposition getWriteDisposition() {
		return writeDisposition;
	}

	public AddLocalTimePipeline setWriteDisposition(BigQueryIO.Write.WriteDisposition writeDisposition) {
		this.writeDisposition = writeDisposition;
		return this;
	}

	public BigQueryIO.Write.CreateDisposition getCreateDisposition() {
		return createDisposition;
	}

	public AddLocalTimePipeline setCreateDisposition(BigQueryIO.Write.CreateDisposition createDisposition) {
		this.createDisposition = createDisposition;
		return this;
	}

	public boolean getWriteData() {
		return writeData;
	}

	public AddLocalTimePipeline setWriteData(boolean writeData) {
		this.writeData = writeData;
		return this;
	}

	/**
	 * Add local time, time zone, zone name and GMT offset to table.
	 * @param utcOnlyTestRows
	 * @return
	 */
	public PCollection<TableRow> apply(PCollection<TableRow> utcOnlyTestRows) {
		
		PCollection<TableRow> data;
		if (utcOnlyTestRows != null) {
			data = utcOnlyTestRows; 
		} else {
			data = this.loadData();
		}

		PCollection<TableRow> timezones = this.pipeline.apply(
			BigQueryIO.Read
				.named("Read " + BQ_TIMEZONE_TABLE)
				.from(BQ_TIMEZONE_TABLE));
		
		// build lookup map
		PCollection<KV<String, TableRow>> zonekeys = 
				timezones.apply(ParDo.named("find key")
						.of(new ExtractZoneKeynameFn()));
		
		PCollection<KV<String, Iterable<TableRow>>> groupedZones = zonekeys.apply(
				    GroupByKey.<String, TableRow>create());
		
		PCollectionView<Map<String, Iterable<TableRow>>> groupedZonesMap = groupedZones.apply(View.asMap());
		
		// resolve time
		PCollection<TableRow> withLocaltimeData = data.apply(
				ParDo
					.named("Add local time")
					.withSideInputs(groupedZonesMap)
					.of(new AddLocalTimeFn(groupedZonesMap)));
		
		if (this.getWriteData()) {

			withLocaltimeData.apply(
				BigQueryIO.Write
					.named("Write to " + this.outputTable)
					.to(this.outputTable)
					.withSchema(this.outputSchema)
					.withWriteDisposition(this.writeDisposition)
					.withCreateDisposition(this.createDisposition));
		}
		
		return withLocaltimeData;
	}
	
	public PCollection<TableRow> apply() {
		return this.apply(null);
	}


	
	public static void main(String [] args) throws ClassNotFoundException {
		// parse out whether we are doing a day or hour update?
		OptionParser parser = PipelineOptionsSetup.setupOptionParser();
		OptionSet cmdOpts = PipelineOptionsSetup.getOptions(parser, args);
	    
	    // set up big query IO options
	    BigQueryOptions pipelineOpts = PipelineOptionsSetup.setupBQOptions(cmdOpts);
	    pipelineOpts.setAppName("AddLocalTime");
		
		// pipeline object
		Pipeline p = Pipeline.create(pipelineOpts);
		
		AddLocalTimePipeline addLocalTime = new AddLocalTimePipeline(p);
		addLocalTime
			.setWriteData(true)
			.setOutputTable(OUTPUT_TABLE)
			.setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
			.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
			.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);
		
		addLocalTime.apply();
		
		p.run();
	}

	
}
