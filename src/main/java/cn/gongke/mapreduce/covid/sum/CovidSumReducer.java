package cn.gongke.mapreduce.covid.sum;

import cn.gongke.mapreduce.covid.beans.CovidCountBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Ben
 * @description DESCRIPTION_INFO
 * @date 2023年10月24日 17:38
 */
public class CovidSumReducer extends Reducer<Text , CovidCountBean , Text , CovidCountBean> {
    private CovidCountBean outV = new CovidCountBean();

    @Override
    protected void reduce(Text key, Iterable<CovidCountBean> values, Reducer<Text, CovidCountBean, Text, CovidCountBean>.Context context) throws IOException, InterruptedException {
        // 统计变量
        long totalCases = 0;
        long totalDeath = 0;
        // 遍历该州的各个县的数据
        for (CovidCountBean value : values) {
            totalCases += value.getCases();
            totalDeath += value.getDeaths();
        }
        // 输出结果的赋值
        outV.set(totalCases , totalDeath);
        // 输出结果
        context.write(key , outV);
    }
}
