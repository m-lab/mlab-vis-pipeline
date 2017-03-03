package mlab.dataviz;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import mlab.dataviz.dofn.AddLocalTimeFn;
import mlab.dataviz.dofn.ExtractZoneKeynameFn;
import mlab.dataviz.util.Schema;
public class AddLocalTimePipeline extends BasePipeline {
	private static final Logger LOG = LoggerFactory.getLogger(AddLocalTimePipeline.class);

	// for running main()
	private static final String INPUT_TABLE = "mlab-oti:data_viz_testing.zz_base_by_day_with_isp_test"; //"mlab-oti:data_viz.base_all_ip_by_day_with_isps";
	private static final String OUTPUT_TABLE = "mlab-oti:data_viz_testing.zz_base_by_day_with_isp_test_localized"; // "mlab-oti:data_viz.base_all_ip_by_day_with_isps_localized";
	private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";

	// zones data
	private static String BQ_TIMEZONE_TABLE = "mlab-oti:data_viz_helpers.localtime_timezones";

	public AddLocalTimePipeline(Pipeline p) {
		super(p);
	}

	/**
	 * Add local time, time zone, zone name and GMT offset to table.
	 * @param utcOnlyTestRows
	 * @return
	 */
	public PCollection<TableRow> applyInner(PCollection<TableRow> data) {
		PCollection<TableRow> timezones = this.pipeline.apply(
			BigQueryIO.Read
				.named("Read " + BQ_TIMEZONE_TABLE)
				.from(BQ_TIMEZONE_TABLE));

		// build lookup map
		PCollection<KV<String, TableRow>> zonekeys =
				timezones.apply(ParDo.named("Extract Timezone Key")
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

		return withLocaltimeData;
	}

	public static void main(String [] args) throws ClassNotFoundException {
		BigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(BigQueryOptions.class);
		options.setAppName("AddLocalTime");

		// pipeline object
		Pipeline p = Pipeline.create(options);

		AddLocalTimePipeline addLocalTime = new AddLocalTimePipeline(p);
		addLocalTime
			.setWriteData(true)
			.setInputTable(INPUT_TABLE)
			.setOutputTable(OUTPUT_TABLE)
			.setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
			.setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
			.setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

		addLocalTime.apply();

		p.run();
	}


}
