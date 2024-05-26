package com.beam.project.amount.runner;

import com.beam.project.amount.pipeline.AmountCalculation;
import com.beam.project.common.LocalFileOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * 业务要求：
 * 1、输入数据及格式：
 * Code,SubCode,TypeCode,Amount,CreateDate
 * B0001,S0001,T1,20240101,Tel,40
 * <p>
 * 2、业务处理逻辑：
 * 2.1 逐行读入而文本数据并转换成AmountRecord对象记录
 * 2.2 根据对象属性Code的最后两位进行splint分割进行分组并行处理
 * 2.3 根据SubCode和Type对分组后的数据进行Amount的小计计算，并将该步骤的结果输出到SubCodeTypeTotal.txt文件
 * 2.4 根据Code和Type对将2.3步骤计算的小计结果进行汇总合计
 * 2.5 将2.4步骤合计结果输出到CodeTypeTotal.txt文件
 */
public class AmountCalculationLocal {

    public static void main(String[] args) {
        LocalFileOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(LocalFileOptions.class);
        options.setRunner(DirectRunner.class);
        options.setInput("./data/project/input/testData.txt");
        options.setOutput("./data/project/output/");

        AmountCalculation calculation = new AmountCalculation(options);
        calculation.runPipeline();
    }
}
