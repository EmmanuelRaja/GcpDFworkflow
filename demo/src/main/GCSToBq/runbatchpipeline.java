import org.apache.beam.sdk.io.TextIO;

public void runBatchPipeline(String inputPath) {
    Pipeline pipeline = Pipeline.create();
    pipeline.apply("ReadFromFile", TextIO.read().from(inputPath))
            .apply("ProcessData", MapElements.via(new SimpleFunction<String, String>() {
                @Override
                public String apply(String input) {
                    // Data processing logic here
                    return input; // or transformed data
                }
            }))
            .apply("WriteToSpanner", SpannerIO.write().withInstanceId("my-instance").withDatabaseId("my-database"));
    pipeline.run();
}
