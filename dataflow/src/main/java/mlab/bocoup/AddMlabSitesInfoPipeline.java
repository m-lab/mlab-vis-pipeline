package mlab.bocoup;

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
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import mlab.bocoup.coder.NavigableMapCoder;
import mlab.bocoup.dofn.AddMlabSitesInfoFn;
import mlab.bocoup.transform.CombineAsNavigableMapHex;
import mlab.bocoup.util.Schema;


/**
 * Pipeline for overriding server locations based on the data provided from
 * https://mlab-ns.appspot.com/admin/sites
 * 
 * Adds in mlab site ID, lat, long, city, region, country, continent
 *
 * @author pbeshai
 *
 */
public class AddMlabSitesInfoPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(AddMlabSitesInfoPipeline.class);

  private static final String INPUT_TABLE = "mlab-oti:bocoup.peter_test";
  private static final String OUTPUT_TABLE = "mlab-oti:bocoup.peter_test_out";

  private static final String MLAB_SITES_TABLE = "mlab-oti:bocoup.mlab_sites";

  private static final String OUTPUT_SCHEMA = "./data/bigquery/schemas/all_ip.json";

  private Pipeline pipeline;
  private String inputTable = INPUT_TABLE;
  private String outputTable = OUTPUT_TABLE;
  private String mlabSitesTable = MLAB_SITES_TABLE;
  private boolean writeData = false;
  private TableSchema outputSchema;
  private BigQueryIO.Write.WriteDisposition writeDisposition = BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
  private BigQueryIO.Write.CreateDisposition createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;

  /**
   * Create a new pipeline.
   * @param p The Dataflow pipeline to build on
   */
  public AddMlabSitesInfoPipeline(Pipeline p) {
    this.pipeline = p;
  }

  /**
   * Initial read of the source data table from BigQuery.
   *
   * @param p The Dataflow Pipeline
   * @return The PCollection representing the data from BigQuery
   */
  public PCollection<TableRow> loadByIpData() {
    // read in the by IP data from `by_ip_day_base`
    PCollection<TableRow> byIpData = this.pipeline.apply(
        BigQueryIO.Read
        .named("Read " + this.inputTable)
        .from(this.inputTable));

    return byIpData;
  }
  
  /**
   * Adds in ISP information for the client.
   *
   * @param p The Dataflow Pipeline
   * @param byIpData The PCollection representing the rows to have ISP information added to them
   * @return A PCollection of rows with ISP information added to them
   */
  public PCollection<TableRow> addMlabSiteInfo(PCollection<TableRow> byIpData, PCollectionView<NavigableMap<String, TableRow>> mlabSitesView) {

    // Use the side-loaded MaxMind ISP data to get server ISPs
    PCollection<TableRow> byIpDataWithISPs = byIpData.apply(
        ParDo
        .named("Add Server ISPs (side input)")
        .withSideInputs(mlabSitesView)
        .of(new AddMlabSitesInfoFn(mlabSitesView)));
    return byIpDataWithISPs;
  }

  /**
   * Write the modified rows to a destination table in BigQuery
   *
   * @param byIpData The PCollection of rows with ISP information added to them already
   * @param outputTable The identifier for the table to write to (e.g. `bocoup.my_table`)
   * @param outputTableSchema The schema describing the output table
   */
  public void writeByIpData(PCollection<TableRow> byIpData) {
    // Write the changes to `by_ip_day`
    byIpData.apply(
        BigQueryIO.Write
        .named("Write to " + this.outputTable)
        .to(this.outputTable)
        .withSchema(this.outputSchema)
        .withWriteDisposition(this.writeDisposition)
        .withCreateDisposition(this.createDisposition));
  }

  /**
   * Adds necessary configuration to a Pipeline for adding ISPs to work.
   *
   * @param p The pipeline being used
   * @return The pipeline being used (for convenience)
   */
  public void preparePipeline() {
    // needed to use NavigableMap as an output/accumulator for adding ISPs efficiently
    this.pipeline.getCoderRegistry().registerCoder(NavigableMap.class, NavigableMapCoder.class);
  }

  /**
   * Add in the steps to add MLab site info to a given pipeline. Writes the
   * data to a table if writeData field is true.
   *
   * @param p A pipeline to add the steps to. If null, a new pipeline is created.
   * @param byIpData If provided, ISPs are added to this collection, otherwise they are read
   * in from a BigQuery table.
   *
   * @return The PCollection with MLab site info added
   */
  public PCollection<TableRow> apply(PCollection<TableRow> byIpData) {
    this.preparePipeline();

    // read in the data from the table unless it has been provided
    PCollection<TableRow> data;
    if (byIpData != null) {
      data = byIpData;
    } else {
      data = this.loadByIpData();
    }
    
    // Read in the MaxMind ISP data
    PCollection<TableRow> mlabSites = this.pipeline.apply(
        BigQueryIO.Read
        .named("Read " + this.mlabSitesTable)
        .from(this.mlabSitesTable));

    //Make the MaxMind ISP data ready for side input
    PCollectionView<NavigableMap<String, TableRow>> mlabSitesView =
        mlabSites
        .apply(Combine.globally(new CombineAsNavigableMapHex())
            .named("Combine ISPs as NavMap")
            // make a view so it can be used as side input
            .asSingletonView());


    // add in ISPs
    data = this.addMlabSiteInfo(data, mlabSitesView);

    if (this.writeData) {
      // write the processed data to a table
      this.writeByIpData(data);
    }

    return data;
  }

  /**
   * Add in the steps to add Mlab Site information to a given pipeline. Reads data from a
   * BigQuery table to begin with. Writes the data to a table if writeData field is
   * true.
   *
   * @return The PCollection with MLab site information added
   */
  public PCollection<TableRow> apply() {
    return this.apply(null);
  }

  public Pipeline getPipeline() {
    return pipeline;
  }


  public AddMlabSitesInfoPipeline setPipeline(Pipeline pipeline) {
    this.pipeline = pipeline;
    return this;
  }


  public String getInputTable() {
    return inputTable;
  }


  public AddMlabSitesInfoPipeline setInputTable(String inputTable) {
    this.inputTable = inputTable;
    return this;
  }


  public String getOutputTable() {
    return outputTable;
  }


  public AddMlabSitesInfoPipeline setOutputTable(String outputTable) {
    this.outputTable = outputTable;
    return this;
  }


  public String getMlabSitesTable() {
    return mlabSitesTable;
  }


  public AddMlabSitesInfoPipeline setMlabSitesTable(String mlabSitesTable) {
    this.mlabSitesTable = mlabSitesTable;
    return this;
  }
  

  public TableSchema getOutputSchema() {
    return outputSchema;
  }


  public AddMlabSitesInfoPipeline setOutputSchema(TableSchema outputSchema) {
    this.outputSchema = outputSchema;
    return this;
  }


  public BigQueryIO.Write.WriteDisposition getWriteDisposition() {
    return writeDisposition;
  }


  public AddMlabSitesInfoPipeline setWriteDisposition(BigQueryIO.Write.WriteDisposition writeDisposition) {
    this.writeDisposition = writeDisposition;
    return this;
  }


  public BigQueryIO.Write.CreateDisposition getCreateDisposition() {
    return createDisposition;
  }


  public AddMlabSitesInfoPipeline setCreateDisposition(BigQueryIO.Write.CreateDisposition createDisposition) {
    this.createDisposition = createDisposition;
    return this;
  }

  public boolean getWriteData() {
    return writeData;
  }


  public AddMlabSitesInfoPipeline setWriteData(boolean writeData) {
    this.writeData = writeData;
    return this;
  }

  /**
   * The main program: start the pipeline, add in ISP information and write it to a table.
   * @param args
   */
  public static void main(String[] args) {
    BigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(BigQueryOptions.class);

    // pipeline object
    Pipeline p = Pipeline.create(options);

    AddMlabSitesInfoPipeline addMlabSiteInfo = new AddMlabSitesInfoPipeline(p);
    addMlabSiteInfo
      .setWriteData(true)
      .setOutputTable(OUTPUT_TABLE)
      .setOutputSchema(Schema.fromJSONFile(OUTPUT_SCHEMA))
      .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
      .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);

    addMlabSiteInfo.apply();

    p.run();
  }
}
