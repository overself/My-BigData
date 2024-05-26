package com.beam.project.common;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface LocalFileOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    @Default.String("/data/beam/input/testData.txt")
    String getInput();

    void setInput(String value);

    @Description("Path of the file to write to")
    @Validation.Required
    @Default.String("/data/beam/output/")
    String getOutput();

    void setOutput(String value);
}
