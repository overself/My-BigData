package com.beam.project.demo.stream;

import com.beam.project.common.SnowFlakeUtil;
import com.beam.project.demo.bean.ExamScore;
import com.beam.project.demo.bean.SchoolClass;
import com.beam.project.demo.bean.Subject;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.joda.time.Instant;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.beam.project.demo.bean.GeneratorUtil.getRandomChineseStr;

/**
 * SCHOOL: "SC01", "SC02", "SC03"
 * CLASS: "SC01", "SC02", "SC03"
 * SUBJECT: "SC01", "SC02", "SC03"
 * STUDENT_SCORE: "SC01"：, "SC02", "SC03"
 * <p>
 * 1、接收消息： SCHOOL、CLASS、时间戳
 * 2、处理触发条件：每3分钟，或者每3条数据
 * 3、数据去重： 去重key：SCHOOL && CLASS
 * 4、获取指定班级全部学生成绩，条件：SCHOOL && CLASS -> SCHOOL、CLASS、SUBJECT、STUDENT、SCORE
 * 5、按照学科统计班级的成绩平均分，分组Key：SCHOOL && CLASS && SUBJECT ->SCHOOL、CLASS、SUBJECT、AVG(SCORE)
 * 6、按照学科统计学校的成绩平均分，分组Key：SCHOOL && SUBJECT ->SCHOOL、SUBJECT、AVG(SCORE)
 * 7、对学校各科成绩进行顺序排名，分组及排序Key：SCHOOL && SUBJECT ->SCHOOL、SUBJECT、SCORE(名次)
 * 8、输出成绩名词数据结果：SCHOOL && SUBJECT ->SCHOOL、SUBJECT、SCORE(名次)
 */
@Slf4j
public class GeneratorScore {

    private static final String[] SCHOOL_CODE = {"SC01", "SC02", "SC03"};

    private static final String[] CLASS_CODE = {"CL01", "CL02", "CL03", "CL04", "CL05", "CL06"};

    private static final String[] SUBJECT_CODE = {/*"maths", "English",*/ "physics", "Chinese"};

    public static void main(String[] args) {
        SchoolClass schoolClass = GeneratorScore.getRandomSchoolClass();
        log.info("学校班级：{}", schoolClass);
        List<ExamScore> scores = GeneratorScore.getSubjectScore(schoolClass);
        log.info("---------------------------------------------------------");
        GeneratorScore.calcSubjectScore(scores, "classes");
        log.info("---------------------------------------------------------");
        GeneratorScore.calcSubjectScore(scores, "school");
    }

    public static SchoolClass getRandomSchoolClass() {
        SchoolClass schoolClass = new SchoolClass();
        schoolClass.setClassCode(CLASS_CODE[RandomUtils.nextInt(0, 5)]);
        schoolClass.setSchoolCode(SCHOOL_CODE[RandomUtils.nextInt(0, 2)]);
        return schoolClass;
    }

    public static List<ExamScore> getSubjectScore(SchoolClass scClass) {
        List<ExamScore> examScores = Lists.newArrayList();
        // 随机测试：获取5到10人的各科成绩
        int studentNums = RandomUtils.nextInt(5, 10);
        // 固定测试
        //int studentNums = 5;
        for (String subject : SUBJECT_CODE) {
            for (int index = 0; index < studentNums; index++) {
                ExamScore score = new ExamScore();
                score.setDataId(SnowFlakeUtil.getSnowFlakeId());
                score.setClassCode(scClass.getClassCode());
                score.setSchoolCode(scClass.getSchoolCode());
                score.setSubject(Subject.fromCode(subject));
                score.setStudentCode(getRandomChineseStr(3));
                score.setScore(BigDecimal.valueOf(RandomUtils.nextInt(65, 100)));
                examScores.add(score);
            }
        }
        return examScores;
    }

    public static List<ExamScore> calcSubjectScore(List<ExamScore> scores, String groupType) {

        if (scores == null || scores.size() == 0) {
            log.info("计算数据为空");
            throw new RuntimeException("计算数据为空");
        }

        Map<String, List<ExamScore>> collectMap;
        if ("School".equalsIgnoreCase(groupType)) {
            collectMap = scores.stream().collect(Collectors.groupingBy(ExamScore::getSchoolSubjectKey));
        } else {
            collectMap = scores.stream().collect(Collectors.groupingBy(ExamScore::getClassSubjectKey));
        }
        List<ExamScore> scoreGroup = Lists.newArrayList();
        for (Map.Entry<String, List<ExamScore>> entry : collectMap.entrySet()) {
            String keys = entry.getKey();
            List<ExamScore> examScores = entry.getValue();
            try {
                ExamScore examScore = examScores.get(0).clone();
                examScore.setScore(BigDecimal.ZERO);
                examScores.forEach(item -> {
                    log.info("Type:{}, Key:{}, Value:{}", "Item", keys, item.getScore());
                    examScore.setScore(examScore.getScore().add(item.getScore()));
                });
                examScore.setScore(examScore.getScore().divide(BigDecimal.valueOf(examScores.size()), 2, RoundingMode.UP));
                examScore.setStudentCode(null);
                if ("School".equals(groupType)) {
                    examScore.setClassCode(null);
                }
                log.info("Type:{}, Key:{}, Value:{}", groupType, keys, examScore.getScore());
                scoreGroup.add(examScore);
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }
        return scoreGroup;

    }

}
