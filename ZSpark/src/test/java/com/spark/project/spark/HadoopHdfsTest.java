package com.spark.project.spark;

import com.spark.project.BootBaseTest;
import com.spark.project.common.HdfsClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.annotation.Resource;

@Slf4j
@TestMethodOrder(value = MethodOrderer.OrderAnnotation.class)
public class HadoopHdfsTest extends BootBaseTest {

    @Resource
    private HdfsClient hdfsService;

    //https://blog.csdn.net/Winter_chen001/article/details/110449133
    //https://blog.csdn.net/linhaiyun_ytdx/article/details/90486277
    @Test
    @Order(1)
    public void uploadFileToHdfsTest() {
        hdfsService.uploadFile("data/WordContentForCount.txt", "/test/hadoop/in/WordContentForCount.txt");
        String text = hdfsService.readFileContent("/test/hadoop/in/WordContentForCount.txt");
        log.info("text==={}", text);
    }

    @Test
    @Order(100)
    public void deleteFileFromHdfsTest() {
        hdfsService.deleteFile("/test/hadoop/in/WordContentForCount.txt");
    }

}
