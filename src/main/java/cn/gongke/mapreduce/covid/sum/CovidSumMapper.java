package cn.gongke.mapreduce.covid.sum;

import cn.gongke.mapreduce.covid.beans.CovidCountBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 重写map方法
 */
public class CovidSumMapper extends Mapper<LongWritable, Text , Text , CovidCountBean> {
    Text outK = new Text();
    CovidCountBean outV = new CovidCountBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取一行数据
        String [] fields = value.toString().split(",");


        // 提取数据，州、 确诊数、死亡数
        outK.set(fields[2]);


        outV.set(Long.parseLong(fields[fields.length -2]) , Long.parseLong(fields[fields.length -1]));

        // 输出结果
        context.write(outK , outV);
    }
}
