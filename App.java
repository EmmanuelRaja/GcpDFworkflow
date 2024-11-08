import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.runners.DirectRunner;

public class MultiFileToSingleSpannerTable {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);  // Use DirectRunner for local testing, switch to DataflowRunner for GCP
        Pipeline pipeline = Pipeline.create(options);

        String projectId = "your-project-id";
        String instanceId = "your-instance-id";
        String databaseId = "your-database-id";
        String tableName = "ConsolidatedData";

        // Define input file paths
        String clientFilePath = "gs://your-bucket/path/to/client.txt";
        String individualFilePath = "gs://your-bucket/path/to/individual.txt";
        String entityFilePath = "gs://your-bucket/path/to/entity.txt";
        String addressFilePath = "gs://your-bucket/path/to/address.txt";

        // Process each file separately and transform data into the consolidated schema

        // Process Client File
        PCollection<Mutation> clientMutations = pipeline
                .apply("ReadClientFile", TextIO.read().from(clientFilePath))
                .apply("ParseClient", ParDo.of(new ParseAndValidateClient()));

        // Process Individual File
        PCollection<Mutation> individualMutations = pipeline
                .apply("ReadIndividualFile", TextIO.read().from(individualFilePath))
                .apply("ParseIndividual", ParDo.of(new ParseAndValidateIndividual()));

        // Process Entity File
        PCollection<Mutation> entityMutations = pipeline
                .apply("ReadEntityFile", TextIO.read().from(entityFilePath))
                .apply("ParseEntity", ParDo.of(new ParseAndValidateEntity()));

        // Process Address File
        PCollection<Mutation> addressMutations = pipeline
                .apply("ReadAddressFile", TextIO.read().from(addressFilePath))
                .apply("ParseAddress", ParDo.of(new ParseAndValidateAddress()));

        // Combine all PCollections into one PCollection of Mutations
        PCollection<Mutation> allMutations = PCollectionList.of(clientMutations)
                .and(individualMutations)
                .and(entityMutations)
                .and(addressMutations)
                .apply(Flatten.pCollections());

        // Write the consolidated data to the single Spanner table
        allMutations.apply("WriteToSpanner", SpannerIO.write()
                .withProjectId(projectId)
                .withInstanceId(instanceId)
                .withDatabaseId(databaseId)
                .withTable(tableName));

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }

    // DoFn for Client data parsing and mapping to the unified schema
    public static class ParseAndValidateClient extends DoFn<String, Mutation> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<Mutation> out) {
            String[] tokens = line.split(",");
            if (tokens.length >= 3) {
                String id = tokens[0].trim();
                String name = tokens[1].trim();
                String lineOfBusiness = tokens[2].trim();

                if (!id.isEmpty() && !name.isEmpty() && !lineOfBusiness.isEmpty()) {
                    Mutation mutation = Mutation.newInsertOrUpdateBuilder("ConsolidatedData")
                            .set("id").to(id)
                            .set("name").to(name)
                            .set("line_of_business").to(lineOfBusiness)
                            .build();
                    out.output(mutation);
                }
            }
        }
    }

    // DoFn for Individual data parsing and mapping to the unified schema
    public static class ParseAndValidateIndividual extends DoFn<String, Mutation> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<Mutation> out) {
            String[] tokens = line.split(",");
            if (tokens.length >= 2) {
                String id = tokens[0].trim();
                String name = tokens[1].trim();

                if (!id.isEmpty() && !name.isEmpty()) {
                    Mutation mutation = Mutation.newInsertOrUpdateBuilder("ConsolidatedData")
                            .set("id").to(id)
                            .set("name").to(name)
                            .build();
                    out.output(mutation);
                }
            }
        }
    }

    // DoFn for Entity data parsing and mapping to the unified schema
    public static class ParseAndValidateEntity extends DoFn<String, Mutation> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<Mutation> out) {
            String[] tokens = line.split(",");
            if (tokens.length >= 2) {
                String id = tokens[0].trim();
                String country = tokens[1].trim();

                if (!id.isEmpty() && !country.isEmpty()) {
                    Mutation mutation = Mutation.newInsertOrUpdateBuilder("ConsolidatedData")
                            .set("id").to(id)
                            .set("country").to(country)
                            .build();
                    out.output(mutation);
                }
            }
        }
    }

    // DoFn for Address data parsing and mapping to the unified schema
    public static class ParseAndValidateAddress extends DoFn<String, Mutation> {
        @ProcessElement
        public void processElement(@Element String line, OutputReceiver<Mutation> out) {
            String[] tokens = line.split(",");
            if (tokens.length >= 2) {
                String id = tokens[0].trim();
                String address = tokens[1].trim();

                if (!id.isEmpty() && !address.isEmpty()) {
                    Mutation mutation = Mutation.newInsertOrUpdateBuilder("ConsolidatedData")
                            .set("id").to(id)
                            .set("address").to(address)
                            .build();
                    out.output(mutation);
                }
            }
        }
    }
}
