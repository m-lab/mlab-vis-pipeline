package mlab.dataviz.pipelines;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import com.google.api.services.bigquery.model.TableRow;

import mlab.dataviz.dofn.AddLocalTimeFn;
import mlab.dataviz.dofn.ExtractZoneKeynameFn;
import mlab.dataviz.pipelineopts.HistoricPipelineOptions;
import mlab.dataviz.util.BQTableUtils;
import mlab.dataviz.util.Schema;

public class AddLocalTimePipeline extends BasePipeline {

	// for running main()
	private static final String INPUT_TABLE = "data_viz_testing.zz_base_by_day_with_isp_test";
	private static final String OUTPUT_TABLE = "data_viz_testing.zz_base_by_day_with_isp_localized_test";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";

	// zones data
	private static String BQ_TIMEZONE_TABLE = "data_viz_helpers.localtime_timezones";

	// bigquery table wrapper helper
	private BQTableUtils bqTableUtils;

	public AddLocalTimePipeline(Pipeline p, BQTableUtils bqTableUtils) {
		super(p);
		this.bqTableUtils = bqTableUtils;
	}

	/**
	 * Add local time, time zone, zone name and GMT offset to table.
	 * 
	 * @param utcOnlyTestRows
	 * @return
	 */
	public PCollection<TableRow> applyInner(PCollection<TableRow> data) {

		String bqTimezoneTable = this.bqTableUtils.wrapTable(BQ_TIMEZONE_TABLE);

		PCollection<TableRow> timezones = this.pipeline.apply("Read " + bqTimezoneTable,
				BigQueryIO.readTableRows().from(bqTimezoneTable));

		// build lookup map
		PCollection<KV<String, TableRow>> zonekeys = timezones.apply("Extract Timezone Key",
				ParDo.of(new ExtractZoneKeynameFn()));

		PCollection<KV<String, Iterable<TableRow>>> groupedZones = zonekeys
				.apply(GroupByKey.<String, TableRow>create());

		PCollectionView<Map<String, Iterable<TableRow>>> groupedZonesMap = groupedZones.apply(View.asMap());

		// resolve time
		PCollection<TableRow> withLocaltimeData = data.apply("Add local time",
				ParDo.of(new AddLocalTimeFn(groupedZonesMap)).withSideInputs(groupedZonesMap));

		return withLocaltimeData;
	}

	public static void main(String[] args) throws ClassNotFoundException {
		BigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryOptions.class);
		options.setAppName("AddLocalTime");

		// pipeline object
		Pipeline p = Pipeline.create(options);
		HistoricPipelineOptions optionsLocalTime = options.as(HistoricPipelineOptions.class);
		BQTableUtils bqTableUtils = new BQTableUtils(optionsLocalTime);

		AddLocalTimePipeline addLocalTime = new AddLocalTimePipeline(p, bqTableUtils);
		addLocalTime.setWriteData(true).setInputTable(bqTableUtils.wrapTable(INPUT_TABLE))
				.setOutputTable(bqTableUtils.wrapTable(OUTPUT_TABLE))
				.setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
				.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
				.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

		addLocalTime.apply();

		p.run();
	}

}
