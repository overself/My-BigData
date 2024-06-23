package com.beam.project.demo.stream.transform;

import com.beam.project.demo.bean.SchoolClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

@Slf4j
public class SchoolClassDistinctTransform extends PTransform<PCollection<SchoolClass>, PCollection<SchoolClass>> {
    @Override
    public PCollection<SchoolClass> expand(PCollection<SchoolClass> input) {
        return input.apply(Distinct.withRepresentativeValueFn(new SimpleFunction<SchoolClass, String>() {
            @Override
            public String apply(SchoolClass input) {
                log.info("#withRepresentativeValueFn Key：{}", input.getSchoolClassKey());
                return input.getSchoolClassKey();
            }
        }));
    }
}
