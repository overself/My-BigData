package com.beam.project.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.Instant;

@Slf4j
public class LogOutput<T> extends DoFn<T, T> {


    private String prefix;

    public LogOutput(String prefix) {
        this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(@Element T Data, ProcessContext context, OutputReceiver<T> receiver,
                               BoundedWindow window, @Timestamp Instant timestamp, PaneInfo paneInfo) throws Exception {
        if (window instanceof IntervalWindow) {
            IntervalWindow intervalWindow = (IntervalWindow) window;
            log.info("{}", intervalWindow);
        }
        log.info("pane:{},contextTimestamp:{},window:{} =>{}: {}", context.pane(), context.timestamp(), window, prefix, Data);
        receiver.output(Data);
    }

}
