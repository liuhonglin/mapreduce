package com.lhl.test.mapreduce.progress.serialization;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 测试数据：
 *  用户id    用户名称    收入  支出
 *  1       李四      1000    0
 *  2       王五      500     300
 *  1       李四      2000    1000
 *  2       王五      500     200
 *
 * 需求：
 *  用户id    用户名称    总收入  总支出    总余额
 *  1       李四      3000       1000     2000
 *  2       王五      1000        500     500
 *
 * Created by HL.L on 2017/11/25.
 */
public class UserMapReduce {

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

    public static void main(String[] args) {

    }
}
