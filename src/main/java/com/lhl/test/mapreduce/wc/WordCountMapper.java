package com.lhl.test.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by HL.L on 2017/11/22.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 得到输入的每一行数据
        String line = value.toString();

        // 分割数据，通过空格来分割（根据自己的文件来用什么符号分割）
        String[] words = line.split(" ");

        // 循环遍历并输出
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
