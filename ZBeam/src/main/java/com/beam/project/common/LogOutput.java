package com.beam.project.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;

@Slf4j
public class LogOutput<T> extends DoFn<T, T> {

    private String prefix;

    public LogOutput(String prefix) {
        this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(@Element T Data, OutputReceiver<T> receiver) throws Exception {
        log.info("{}: {}", prefix, Data);
        receiver.output(Data);
    }

}
