package com.beam.project.demo.basic.transform;

import com.beam.project.demo.bean.ClassInfo;
import com.beam.project.demo.bean.Student;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class ClassNameFullForStudentDoFn extends DoFn<Student, Student> {

    final PCollectionView<Map<String, ClassInfo>> sideClassData;

    public ClassNameFullForStudentDoFn(PCollectionView<Map<String, ClassInfo>> sideClassData) {
        this.sideClassData = sideClassData;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws CloneNotSupportedException {
        Student student = c.element();
        Student output = student.clone();
        Map<String, ClassInfo> classInfos = c.sideInput(sideClassData);
        if (classInfos==null){
            throw new RuntimeException("请通过withSideInput指定侧边输入对象：PCollectionView<Map<String, ClassInfo>>");
        }
        if (StringUtils.isNotBlank(output.getClassCode())){
            ClassInfo classInfo = classInfos.get(output.getClassCode());
            if (classInfo != null) {
                output.setClassName(classInfo.getClassName());
            }
        }
        c.output(output);
    }
}