package com.beam.project.common;

import org.apache.beam.sdk.PipelineResult;

public interface PipelineRunner {

    PipelineResult.State runPipeline();
}
