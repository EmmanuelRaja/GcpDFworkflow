public class DataQualityCheck extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply("ValidateData", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                String data = context.element();
                // Perform data quality checks (e.g., null checks, format checks)
                if (isValid(data)) {
                    context.output(data);
                } else {
                    // Handle invalid data (log it, send to a dead-letter topic, etc.)
                }
            }
        }));
    }

    private boolean isValid(String data) {
        // Validation logic here
        return true; // or false based on checks
    }
}
