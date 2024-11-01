import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRow;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.util.regex.Pattern;

public class DataQualityPipeline {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadData", TextIO.read().from("gs://your-bucket/input-data.csv"))
                .apply("ParseData", ParDo.of(new ParseTableRowFn()))
                .apply("FilterNullAndMissingValues", ParDo.of(new NullAndMissingValueCheckFn()))
                .apply("FilterDuplicates", new RemoveDuplicates())
                .apply("ValidateRangeThresholds", ParDo.of(new RangeAndThresholdCheckFn()))
                .apply("ValidatePatternMatching", ParDo.of(new PatternMatchingCheckFn()))
                .apply("DateTimeConsistencyCheck", ParDo.of(new DateTimeConsistencyCheckFn()))
                .apply("CustomBusinessRuleValidation", ParDo.of(new CustomBusinessRuleCheckFn()))
                .apply("WriteValidDataToBigQuery", BigQueryIO.writeTableRows()
                        .to("your-project:your_dataset.your_table")
                        .withSchema( /* Define BigQuery schema here */ )
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND))
                .apply("WriteInvalidDataToGCS", TextIO.write().to("gs://your-bucket/invalid-records"));

        pipeline.run().waitUntilFinish();
    }

    // Function to parse input data into TableRow format
    public static class ParseTableRowFn extends DoFn<String, TableRow> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<TableRow> out) {
            String[] values = element.split(",");
            TableRow row = new TableRow()
                    .set("userId", values[0])
                    .set("name", values[1])
                    .set("email", values[2])
                    .set("price", Double.parseDouble(values[3]))
                    .set("startDate", values[4])
                    .set("endDate", values[5]);
            out.output(row);
        }
    }

    // Null and Missing Value Check
    public static class NullAndMissingValueCheckFn extends DoFn<TableRow, TableRow> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
            if (row.get("userId") != null && row.get("email") != null && row.get("price") != null) {
                out.output(row);
            } else {
                // Log or output to invalid data sink
                System.out.println("Missing values in row: " + row.toString());
            }
        }
    }

    // Duplicate Detection (using userId as unique identifier)
    public static class RemoveDuplicates extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
        @Override
        public PCollection<TableRow> expand(PCollection<TableRow> input) {
            return input.apply(Distinct.withRepresentativeValueFn((SerializableFunction<TableRow, String>) row -> row.get("userId").toString()));
        }
    }

    // Range and Threshold Validation (for "price" field)
    public static class RangeAndThresholdCheckFn extends DoFn<TableRow, TableRow> {
        private static final double MIN_PRICE = 0.0;
        private static final double MAX_PRICE = 1000.0;

        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
            Double price = (Double) row.get("price");
            if (price >= MIN_PRICE && price <= MAX_PRICE) {
                out.output(row);
            } else {
                System.out.println("Out of range price: " + row.toString());
            }
        }
    }

    // Pattern Matching Check for Email
    public static class PatternMatchingCheckFn extends DoFn<TableRow, TableRow> {
        private static final Pattern EMAIL_PATTERN = Pattern.compile("^[A-Za-z0-9+_.-]+@(.+)$");

        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
            String email = (String) row.get("email");
            if (EMAIL_PATTERN.matcher(email).matches()) {
                out.output(row);
            } else {
                System.out.println("Invalid email format: " + row.toString());
            }
        }
    }

    // Date and Time Consistency Check (startDate should be before endDate)
    public static class DateTimeConsistencyCheckFn extends DoFn<TableRow, TableRow> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
            String startDate = (String) row.get("startDate");
            String endDate = (String) row.get("endDate");

            if (startDate.compareTo(endDate) < 0) {
                out.output(row);
            } else {
                System.out.println("Inconsistent date in row: " + row.toString());
            }
        }
    }

    // Custom Business Rule Validation (e.g., TotalPrice = unitPrice * quantity)
    public static class CustomBusinessRuleCheckFn extends DoFn<TableRow, TableRow> {
        @ProcessElement
        public void processElement(@Element TableRow row, OutputReceiver<TableRow> out) {
            Double price = (Double) row.get("price");
            int quantity = (Integer) row.get("quantity");
            Double expectedTotalPrice = price * quantity;

            if (expectedTotalPrice.equals(row.get("totalPrice"))) {
                out.output(row);
            } else {
                System.out.println("Business rule violation: " + row.toString());
            }
        }
    }
}
