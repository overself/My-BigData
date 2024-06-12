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
    public void processElement(@Element T Data, ProcessContext context, OutputReceiver<T> receiver) throws Exception {
        log.info("pane:{},timestamp:{}=>{}: {}", context.pane().getNonSpeculativeIndex(), context.timestamp(), prefix, Data);
        receiver.output(Data);
    }

}
