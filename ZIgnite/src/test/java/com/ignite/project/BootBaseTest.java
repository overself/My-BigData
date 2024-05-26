package com.ignite.project;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(classes = {IgniteApplication.class})
public class BootBaseTest {


    @BeforeAll
    public static void runBefore() {
        log.info("Before All Test Method");
    }

}
