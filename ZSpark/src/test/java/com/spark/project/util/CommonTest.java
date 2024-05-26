package com.spark.project.util;


import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.StringTokenizer;

@Slf4j
public class CommonTest {

    @Test
    public void testStringTokenizer(){
        StringTokenizer tokenizer = new StringTokenizer("Apache Spark - A Unified engine for large-scale data analytics\n" +
                "\n" +
                "Apache Spark is a unified analytics engine for large-scale data processing. It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs. It also supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing, pandas API on Spark for pandas workloads, MLlib for machine learning, GraphX for graph processing, and Structured Streaming for incremental computation and stream processing.\n" +
                "\n" +
                "Quick Start\n" +
                "\n" +
                "This tutorial provides a quick introduction to using Spark. We will first introduce the API through Spark’s interactive shell (in Python or Scala), then show how to write applications in Java, Scala, and Python.\n" +
                "\n" +
                "To follow along with this guide, first, download a packaged release of Spark from the Spark website. Since we won’t be using HDFS, you can download a package for any version of Hadoop.\n" +
                "\n" +
                "Note that, before Spark 2.0, the main programming interface of Spark was the Resilient Distributed Dataset (RDD). After Spark 2.0, RDDs are replaced by Dataset, which is strongly-typed like an RDD, but with richer optimizations under the hood. The RDD interface is still supported, and you can get a more detailed reference at the RDD programming guide. However, we highly recommend you to switch to use Dataset, which has better performance than RDD. See the SQL programming guide to get more information about Dataset.",
                "\t");
        log.info("tokenizer.countTokens={}",tokenizer.countTokens());
        while (tokenizer.hasMoreTokens()){
            log.info("tokenizer.nextToken={}",tokenizer.nextToken());
        }
    }

}
