package com.lhl.test.mapreduce.wc_old;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * Created by HL.L on 2017/11/22.
 */
public class WordCountMapReduce {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        JobConf conf = new JobConf(WordCountMapReduce.class);
        conf.setJobName("job_wordcount_old");

        FileInputFormat.addInputPath(conf, new Path("words"));
        FileOutputFormat.setOutputPath(conf, new Path("wc_output2"));

        conf.setMapperClass(WordCountMapper.class);
        conf.setReducerClass(WordCountReduce.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
    }
}
