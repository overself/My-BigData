package com.beam.project.demo.stream.transform;

import com.beam.project.demo.bean.SchoolClass;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class SchoolClassDistinctTransform extends PTransform<PCollection<SchoolClass>, PCollection<SchoolClass>> {
    @Override
    public PCollection<SchoolClass> expand(PCollection<SchoolClass> input) {
        return input.apply(Distinct.withRepresentativeValueFn(new SimpleFunction<SchoolClass, String>() {
            @Override
            public String apply(SchoolClass input) {
                return input.getSchoolClassKey();
            }
        }));
    }
}
