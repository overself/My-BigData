package com.beam.project.amount.pipeline;

import com.beam.project.amount.bean.AmountRecord;
import com.beam.project.amount.transforms.ParseAmountFn;
import com.beam.project.common.LocalFileOptions;
import com.beam.project.common.LogOutput;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class AmountCalculation {

    private LocalFileOptions options;

    private Pipeline pipeline;

    private AmountCalculation(){

    }

    public AmountCalculation(LocalFileOptions options) {
        this.options = options;
        this.pipeline = Pipeline.create(options);
    }

    public void runPipeline() {

        //1、读取文本数据并转换成ExpenseRecord对象
        PCollection<String> dataLines = pipeline.apply("ReadAmountRecords", TextIO.read().from(options.getInput()));
        PCollection<AmountRecord> records = dataLines.apply("ParseReadAmountRecord", ParDo.of(new ParseAmountFn()));
        //records.apply(ParDo.of(new LogOutput<>("PrintReadAmountRecord")));

        //3 根据Code+subCode+type对分组后的数据进行Amount的小计计算，并将该步骤的结果输出到codeTypeTotal.txt文件
        PCollection<KV<String, Long>> partialAmountsForSCode =
                records.apply("MapPartialValue",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                                .via(record -> KV.of(record.getCode() + "," + record.getSubCode() + "," + record.getType(), record.getAmount()))
                ).apply("SumPartialByKey", Sum.longsPerKey());

        //3 根据subCode+Type对分组后的数据进行Amount的小计计算，并将该步骤的结果输出到SubCodeTypeTotal.txt文件
        PCollection<KV<String, Long>> sCodeAndTypeTotals = partialAmountsForSCode
                .apply("MapToSubCodeType",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                                .via(record -> {
                                    String[] keys = record.getKey().split(",");
                                    return KV.of(keys[1] + "," + keys[2], record.getValue());
                                }))
                .apply("SumByToSubCodeType", Sum.longsPerKey());
        sCodeAndTypeTotals
                .apply("FormatSubCodeTypeResult",
                        MapElements.into(TypeDescriptors.strings()).via(kv -> kv.getKey() + "：" + kv.getValue()))
                .apply("WriteSubCodeTypeTotal",
                        TextIO.write().to(options.getOutput() + "SubCodeTypeTotal")
                                .withoutSharding().withNumShards(1).withSuffix(".txt"));

        //4 根据Code+Type对将3步骤计算的小计结果进行汇总合计
        PCollection<KV<String, Long>> bizAndTypeTotals = partialAmountsForSCode
                .apply("ExtractCodeType",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                                .via(kv -> {
                                    String[] keys = kv.getKey().split(",");
                                    return KV.of(keys[0] + "," + keys[2], kv.getValue());
                                }))
                .apply("SumCodeType", Sum.longsPerKey());

        // 5 将4步骤合计结果输出到BizAndTypeTotal.txt文件
        bizAndTypeTotals.apply("FormatCodeTypeResult",
                        MapElements.into(TypeDescriptors.strings())
                                .via(kv -> kv.getKey() + "," + kv.getValue()))
                .apply("WriteCodeTypeTotal",
                        TextIO.write().to(options.getOutput() + "CodeTypeTotal")
                                .withoutSharding().withNumShards(1).withSuffix(".txt"));

        pipeline.run().waitUntilFinish();
    }
}
