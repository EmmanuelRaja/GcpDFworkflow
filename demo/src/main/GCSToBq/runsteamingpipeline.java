import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptor;

public void runStreamingPipeline(String projectId, String subscriptionId) {
    Pipeline pipeline = Pipeline.create();
    pipeline.apply("ReadFromPubSub", PubsubIO.readStrings().fromSubscription("projects/" + projectId + "/subscriptions/" + subscriptionId))
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
