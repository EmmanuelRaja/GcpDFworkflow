package com.example;

import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.MessageType;

public class GCSParquetToSpannerPipeline {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(MyPipelineOptions.class);
        MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        MessageType schema = (MessageType) options.getSchema();
        PCollection<SimpleGroup> parquetData = pipeline.apply("ReadParquetFromGCS", 
            ParquetIO.read(new SimpleGroupFactory(schema))
                .from(options.getInputFile()));

        parquetData.apply("TransformToSpannerMutation", ParDo.of(new DoFn<SimpleGroup, Mutation>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                SimpleGroup record = c.element();

                String empId = record.getString("client_id", 0);
                String empName = record.getString("client_name", 0);
                String email = record.getString("client_email", 0);

                Mutation mutation = Mutation.newInsertOrUpdateBuilder("Employee")
                        .set("client_id").to(empId)
                        .set("client_name").to(empName)
                        .set("client_email").to(email)
                        .build();

                c.output(mutation);
            }
        }))
        .apply("WriteToSpanner", SpannerIO.write()
            .withInstanceId(options.getInstanceId())
            .withDatabaseId(options.getDatabaseId())
            .withProjectId(options.getProjectId()));

        pipeline.run().waitUntilFinish();
    }
}
