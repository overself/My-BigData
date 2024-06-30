package com.beam.project.core.convert;

import com.beam.project.common.SnowFlakeUtil;
import lombok.SneakyThrows;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public class ObjectMapperJson {

    static CustomObjectMapper objectMapper = new CustomObjectMapper();

    public static class WithKeyDataStingConvertDoFn<T> extends DoFn<T, KV<Long, String>> {

        @SneakyThrows
        @ProcessElement
        public void process(@Element T element, OutputReceiver<KV<Long, String>> receiver) {
            Long dataKey = SnowFlakeUtil.getSnowFlakeId();
            String dataValue = objectMapper.writeValueAsString(element);
            receiver.output(KV.of(dataKey, dataValue));
        }
    }

    public static class NonKeyDataStringConvertDoFn<T> extends DoFn<T, String> {

        @SneakyThrows
        @ProcessElement
        public void process(@Element T element, OutputReceiver<String> receiver) {
            receiver.output(objectMapper.writeValueAsString(element));
        }
    }


    public static class ConvertToRecodeDoFn<T> extends DoFn<KV<Long, String>, T> {

        final Class<T> clazz;

        public ConvertToRecodeDoFn(Class<T> clazz) {
            this.clazz = clazz;
        }

        @SneakyThrows
        @ProcessElement
        public void processElement(ProcessContext context) {
            KV<Long, String> input = context.element();
            context.output(objectMapper.readValue(input.getValue(), clazz));
        }
    }

    public static class ConvertToRecodeFunction<T> extends SimpleFunction<KV<Long, String>, T> {

        final Class<T> clazz;

        public ConvertToRecodeFunction(Class<T> clazz) {
            this.clazz = clazz;
        }

        @SneakyThrows
        @Override
        public T apply(KV<Long, String> input) {
            return objectMapper.readValue(input.getValue(), clazz);
        }
    }

    public static class StrToRecodeFunction<T> extends DoFn<String, T> {

        final Class<T> clazz;

        public StrToRecodeFunction(Class<T> clazz) {
            this.clazz = clazz;
        }

        @SneakyThrows
        @ProcessElement
        public void processElement(ProcessContext context) {
            String input = context.element();
            context.output(objectMapper.readValue(input, clazz));
        }
    }

}
