package com.lhl.test.mapreduce.progress.toolrunner;

import com.lhl.test.mapreduce.progress.serialization.UserMapReduce;
import com.lhl.test.mapreduce.progress.serialization.UserWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

/**
 * Created by liuhonglin on 2017/11/28.
 */
public class UserCount extends Configured implements Tool {

    public static class UserMaper extends Mapper<LongWritable, Text, LongWritable, UserWritable> {

        UserWritable userWritable = new UserWritable();
        LongWritable id = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 获取每行数据
            String line = value.toString();

            // 解析每行数据，并封装进JavaBean中
            String[] words = line.split("\t");
            if (words.length == 4) {
                id.set(Long.parseLong(words[0]));

                userWritable.setId(Long.parseLong(words[0]))
                        .setName(words[1])
                        .setInCome(Integer.parseInt(words[2]))
                        .setExpenses(Integer.parseInt(words[3]))
                        .setSum(Integer.parseInt(words[2]) - Integer.parseInt(words[3]));

                context.write(id, userWritable);
            }
        }
    }

    public static class UserReduce extends Reducer<LongWritable, UserWritable, UserWritable, NullWritable> {

        /**
         * 输入数据：
         *  <1,[user01{1,李四,1000,0},user01{1,李四,2000,1000}]>
         *  <2,[user02{2,王五,500,300},user02{2,王五,500,200}]>
         */

        private UserWritable userWritable = new UserWritable();
        private NullWritable n = NullWritable.get();

        @Override
        protected void reduce(LongWritable key, Iterable<UserWritable> values, Context context) throws IOException, InterruptedException {

            String name = "";
            int income = 0;
            int expenses = 0;
            for (UserWritable user : values) {
                name = user.getName();
                income += user.getInCome();
                expenses += user.getExpenses();
            }
            int sum = income - expenses;
            userWritable.setId(key.get())
                    .setName(name)
                    .setInCome(income)
                    .setExpenses(expenses)
                    .setSum(sum);
            context.write(userWritable, n);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "userJob2");

        job.setJarByClass(UserCount.class);
        job.setMapperClass(UserMaper.class);
        job.setReducerClass(UserReduce.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(UserWritable.class);

        job.setOutputKeyClass(UserWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("usercount"));

        FileSystem fs = FileSystem.get(URI.create("hdfs://10.127.92.182:9000"), conf);
        fs.deleteOnExit(new Path("useroutput2"));

        FileOutputFormat.setOutputPath(job, new Path("useroutput2"));

        boolean result = job.waitForCompletion(true);

        return result ? 0 : -1;
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("mapreduce.job.ubertask.enable", true);

        int result = ToolRunner.run(conf, new UserCount(), args);
        System.exit(result);
    }

}
