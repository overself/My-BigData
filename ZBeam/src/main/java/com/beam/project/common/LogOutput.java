package com.beam.project.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.Instant;

@Slf4j
public class LogOutput<T> extends DoFn<T, T> {


    private String prefix;

    public LogOutput(String prefix) {
        this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(@Element T Data, ProcessContext context, BoundedWindow window, @Timestamp Instant timestamp, PaneInfo paneInfo, OutputReceiver<T> receiver) throws Exception {
        log.info("pane:{},contextTimestamp:{},window:{},timestamp:{},paneInfo:{} =>{}: {}", context.pane(), context.timestamp(), window, timestamp, paneInfo, prefix, Data);
        receiver.output(Data);
    }

}
