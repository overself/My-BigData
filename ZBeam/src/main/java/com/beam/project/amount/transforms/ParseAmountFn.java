package com.beam.project.amount.transforms;

import com.beam.project.amount.bean.AmountRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.springframework.util.StringUtils;

import java.time.LocalDate;

public class ParseAmountFn extends DoFn<String, AmountRecord> {

    private final Counter emptyLines = Metrics.counter(ParseAmountFn.class, "emptyLines");

    private final Distribution lineLenDist = Metrics.distribution(ParseAmountFn.class, "lineLenDistro");

    @ProcessElement
    public void processElement(@Element String lineData, OutputReceiver<AmountRecord> receiver) {
        lineLenDist.update(lineData.length());
        if (lineData.trim().isEmpty()) {
            emptyLines.inc();
        }
        String[] parts = lineData.split(",");
        AmountRecord record = new AmountRecord();
        record.setCode(parts[0]);
        record.setSubCode(parts[1]);
        record.setType(parts[2]);
        record.setAmount(Long.parseLong(parts[3]));
        if(StringUtils.hasText(parts[4])){
            record.setCreateDate(LocalDate.parse(parts[4]));
        }
        receiver.output(record);
    }
}
