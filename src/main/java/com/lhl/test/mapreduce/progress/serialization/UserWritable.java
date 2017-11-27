package com.lhl.test.mapreduce.progress.serialization;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by HL.L on 2017/11/25.
 */
public class UserWritable implements Writable {

    /** 用户id */
    private long id;
    /** 用户姓名 */
    private String name;
    /** 收入 */
    private int inCome;
    /** 支出 */
    private int expenses;
    /** 剩余总额 */
    private int sum;

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(inCome);
        dataOutput.writeInt(expenses);
        dataOutput.writeInt(sum);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readLong();
        this.name = dataInput.readUTF();
        this.inCome = dataInput.readInt();
        this.expenses = dataInput.readInt();
        this.sum = dataInput.readInt();
    }

    public long getId() {
        return id;
    }

    public UserWritable setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public UserWritable setName(String name) {
        this.name = name;
        return this;
    }

    public int getInCome() {
        return inCome;
    }

    public UserWritable setInCome(int inCome) {
        this.inCome = inCome;
        return this;
    }

    public int getExpenses() {
        return expenses;
    }

    public UserWritable setExpenses(int expenses) {
        this.expenses = expenses;
        return this;
    }

    public int getSum() {
        return sum;
    }

    public UserWritable setSum(int sum) {
        this.sum = sum;
        return this;
    }

    @Override
    public String toString() {
        return id + "\t" + name + '\t' + inCome + "\t" + expenses + "\t" + sum;
    }
}
