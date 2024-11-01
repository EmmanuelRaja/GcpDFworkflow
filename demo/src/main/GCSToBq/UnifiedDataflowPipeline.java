import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class UnifiedDataflowPipeline {

    public interface UnifiedPipelineOptions extends PipelineOptions, StreamingOptions {
        String getInputFile();
        void setInputFile(String value);

        String getPubSubTopic();
        void setPubSubTopic(String value);

        String getBigQueryTable();
        void setBigQueryTable(String value);
    }

    public static void main(String[] args) {
        // Define Pipeline Options
        UnifiedPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(UnifiedPipelineOptions.class);

        // Set job name based on the mode
        options.setJobName(options.isStreaming() ? "streaming-parquet-to-bigquery" : "batch-parquet-to-bigquery");

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        PCollection<TableRow> records;

        if (options.isStreaming()) {
            // Streaming: Read from Pub/Sub
            records = pipeline
                .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getPubSubTopic()))
                .apply("ProcessPubSubMessage", ParDo.of(new ParseJsonToTableRow()));
        } else {
            // Batch: Read Parquet files from GCS
            records = pipeline
                .apply("ReadParquetFiles", ParquetIO.read(MyDataModel.class).from(options.getInputFile()))
                .apply("ConvertToTableRow", ParDo.of(new DoFn<MyDataModel, TableRow>() {
                    @ProcessElement
                    public void processElement(@Element MyDataModel record, OutputReceiver<TableRow> out) {
                        TableRow row = new TableRow()
                            .set("field1", record.getField1())
                            .set("field2", record.getField2())
                            .set("field3", record.getField3());
                        out.output(row);
                    }
                }));
        }

        // Write to BigQuery
        records.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
            .to(options.getBigQueryTable())
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withSchema(new TableSchema().setFields(Arrays.asList(
                new TableFieldSchema().setName("field1").setType("STRING"),
                new TableFieldSchema().setName("field2").setType("INTEGER"),
                new TableFieldSchema().setName("field3").setType("FLOAT")
            )))
        );

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }

    // Custom ParDo function to parse JSON messages from Pub/Sub and convert them to TableRows
    public static class ParseJsonToTableRow extends DoFn<String, TableRow> {
        @ProcessElement
        public void processElement(@Element String json, OutputReceiver<TableRow> out) {
            // Convert the JSON to a TableRow (you may need to use a library like Gson or Jackson)
            TableRow row = new TableRow();
            // Parse your JSON here and populate TableRow with field1, field2, and field3
            // row.set("field1", ...);
            // row.set("field2", ...);
            // row.set("field3", ...);
            out.output(row);
        }
    }
}
