package com.spark.project.demo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * 从指定单词范围内随机选择，生成一个文本文件,输出要求如下
 * 1、每行输出最多10个单词，超过30个换行
 * 2、单词之间用空格分开
 * 3、生成文件大小可以指定，如1M，3M
 * 可用单词范围可指定，如："The","Apache","Hadoop","software"
 */
public class TextFileGenerator {

    private static final int min = 1;

    private static final int max = 6;

    private static final String[] words = {"The", "Apache", "Hadoop", "software", "library", "framework", "that", "allows", "fothe", "distributed", "processing", "large", "data", "sets", "across", "clusters", "computers", "using", "simple", "programming", "models", "It", "designed", "scale", "up", "from", "single", "servers", "thousands", "machines", "each", "offering", "local", "computatiand", "storage", "Rathethrely", "hardware", "delivehigh-availability", "the", "library", "itself", "designed", "detect", "and", "handle", "failures", "at", "the", "applicatilaye", "so", "delivering", "highly-available", "service", "top", "clustecomputers", "each", "which", "may", "be", "prone", "failures", "Apache", "Spark", "unified", "analytics", "engine", "folarge-scale", "data", "processing", "It", "provides", "high-level", "APin", "Java", "Scala", "Pythand", "and", "optimized", "engine", "that", "supports", "general", "executigraphs", "It", "also", "supports", "rich", "set", "higher-level", "tools", "including", "Spark", "SQL", "NoSQL", "and", "structured", "data", "processing", "pandas", "API", "Spark", "fopandas", "workloads", "MLlib", "fomachine", "learning", "GraphX", "fograph", "processing", "and", "Structured", "Streaming", "foincremental", "computatiand", "stream", "processing"};

    public static void generateTextFile(String fileName, long fileSizeInBytes) throws IOException {
        File file = new File(fileName);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdir();
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            Random random = new Random();

            long bytesWritten = 0;
            while (bytesWritten < fileSizeInBytes) {
                StringBuilder line = new StringBuilder();

                // 生成每行至少5个单词，最多10个单词
                int numWordsInLine = random.nextInt(max) + min;
                if (bytesWritten + numWordsInLine * 2 > fileSizeInBytes) {
                    break; // 如果剩余的字节数不足以容纳最小数量的单词，则停止生成
                }
                for (int i = 0; i < numWordsInLine; i++) {
                    // 随机选择单词
                    String word = words[random.nextInt(words.length)];
                    // 将单词写入行，并添加逗号分隔符
                    line.append(word).append(" ");
                }

                // 移除行末尾的逗号
                line.deleteCharAt(line.length() - 1);

                // 写入行，并加上换行符
                writer.write(line.toString());
                writer.newLine();

                // 更新已写入字节数
                bytesWritten += line.length() + 2; // 加上逗号和换行符的字节数
            }
        }
    }

    public static void main(String[] args) {
        try {
            //generateTextFile("output8K.txt", 8192);
            generateTextFile("/data/output_" + System.currentTimeMillis() + "512M.txt", 1048576 * 512); //生成1MB大小的文件1048576
            generateTextFile("/data/output_" + System.currentTimeMillis() + "1G.txt", 1048576 * 1024); //生成1MB大小的文件1048576
            //generateTextFile("output1G.txt", 1048576 * 1024 * 1); //生成1MB大小的文件1048576
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
