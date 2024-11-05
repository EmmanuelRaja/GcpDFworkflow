package com.example;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface MyPipelineOptions extends PipelineOptions {
    @Description("gs://mybucketdemo/target/data.parquet")
    String getInputFile();
    void setInputFile(String value);

    @Description("mySpannerInstanceId")
    String getInstanceId();
    void setInstanceId(String value);

    @Description("mySpannerDBId")
    String getDatabaseId();
    void setDatabaseId(String value);

    @Description("mydemoprojectId")
    String getProjectId();
    void setProjectId(String value);
}
