package com.beam.project.demo.bean;

import com.google.api.client.util.Lists;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Random;

public class GeneratorUtil {

    public static List<ClassInfo> getClassInfos() {
        List<ClassInfo> classDataList = Lists.newArrayList();
        classDataList.add(new ClassInfo("C001", "一班"));
        classDataList.add(new ClassInfo("C002", "二班"));
        classDataList.add(new ClassInfo("C003", "三班"));
        classDataList.add(new ClassInfo("C004", "四班"));
        classDataList.add(new ClassInfo("C005", "五班"));
        classDataList.add(new ClassInfo("C006", "六班"));
        return classDataList;
    }

    public static List<Student> getNoClassStudents() {
        List<Student> studentDataList = Lists.newArrayList();
        studentDataList.add(new Student("S007", "刘强", null));
        studentDataList.add(new Student("S008", "刘岷", "C007"));
        return studentDataList;
    }

    public static List<Student> getClassStudents() {
        List<Student> studentDataList = Lists.newArrayList();
        studentDataList.add(new Student("S001", "张一", "C001"));
        studentDataList.add(new Student("S002", "张二", "C001"));
        studentDataList.add(new Student("S003", "张三", "C002"));
        studentDataList.add(new Student("S004", "王五", "C001"));
        studentDataList.add(new Student("S005", "王莽", "C002"));
        studentDataList.add(new Student("S006", "李司", "C003"));
        return studentDataList;
    }

    public static List<Student> getAllStudents() {
        List<Student> dataList = getClassStudents();
        dataList.addAll(getNoClassStudents());
        return dataList;
    }

    public static List<Student> getClassStudents(int fromCode, int endCode) {
        List<Student> studentDataList = Lists.newArrayList();
        for (int loop = fromCode; loop <= endCode; loop++) {
            String code = getRandomChineseStr(3);
            studentDataList.add(new Student("S00" + fromCode, code, "C00" + RandomUtils.nextInt(1, 6)));
        }
        return studentDataList;
    }

    public static String getRandomChineseStr(int length) {
        if (length < 1) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            int high = 0x9FA5; // 设置高位范围，9FA5是汉字的最大编码
            int low = 0x4E00; // 设置低位范围，4E00是汉字的最小编码
            int code = (random.nextInt(high - low + 1) + low);
            sb.append((char) code);
        }
        return sb.toString();
    }
}
