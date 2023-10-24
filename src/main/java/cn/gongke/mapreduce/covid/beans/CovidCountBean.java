package cn.gongke.mapreduce.covid.beans;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Ben
 * @description 自定义对象作为数据类型在MapReduce中传递，别忘了实现Hadoop的序列化机制
 * @date 2023年10月24日 17:27
 */
public class CovidCountBean implements Writable {
    private long cases; // 确诊病例数

    private long deaths; // 死亡病例数

    public CovidCountBean() {
    }
    // 自己疯转一个对现给的set方法，
    public void set(long cases, long deaths) {
        this.cases = cases;
        this.deaths = deaths;
    }

    public long getCases() {
        return cases;
    }

    public void setCases(long cases) {
        this.cases = cases;
    }

    public long getDeaths() {
        return deaths;
    }

    public void setDeaths(long deaths) {
        this.deaths = deaths;
    }

    @Override
    public String toString() {
        return cases + "\t" + deaths;
    }

    /**
     * 序列化方法：可以控制将那些字段序列化出去
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        // 先进去的先出来
        out.writeLong(cases);
        out.writeLong(deaths);
    }

    /**
     * 反序列化方法：todo 注意反序列化的顺序和序列化一致
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 顺序与实现的write方法中的字段的顺序保持一致
        this.cases = dataInput.readLong();
        this.deaths = dataInput.readLong();
    }
}
