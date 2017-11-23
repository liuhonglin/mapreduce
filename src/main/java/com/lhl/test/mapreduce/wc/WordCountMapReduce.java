package com.lhl.test.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by HL.L on 2017/11/22.
 */
public class WordCountMapReduce {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        // 创建配置对象
        Configuration conf = new Configuration();

        // 创建Job对象
        Job job = Job.getInstance(conf, "job_wordcount");

        // 设置运行job的主类
        job.setJarByClass(WordCountMapReduce.class);

        // 设置mapper类
        job.setMapperClass(WordCountMapper.class);
        // 设置reduce类
        job.setReducerClass(WordCountReduce.class);

        // 设置map输出的key value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置reduce输出的key value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入输出的路径
        FileInputFormat.addInputPath(job, new Path("words"));
        FileOutputFormat.setOutputPath(job, new Path("wc_output1"));

        // 提交job
        boolean b = job.waitForCompletion(true);

        if (!b) {
            System.out.println("this task failed!!!");
        }

    }
}
