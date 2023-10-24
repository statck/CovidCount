package cn.gongke.mapreduce.covid.sum;


import cn.gongke.mapreduce.covid.beans.CovidCountBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

/**
 * @description: 美国各县新冠疫情汇总统计 客户端驱动类
 */
public class CovidSumDriver {
    public static void main(String[] args) throws Exception{
        //配置文件对象
        Configuration conf = new Configuration();
        // 创建作业实例
        Job job = Job.getInstance(conf, CovidSumDriver.class.getSimpleName());
        // 设置作业驱动类
        job.setJarByClass(CovidSumDriver.class);

        // 设置作业mapper reducer类
        job.setMapperClass(CovidSumMapper.class);
        job.setReducerClass(CovidSumReducer.class);

        // 设置作业mapper阶段输出key value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CovidCountBean.class);
        //设置作业reducer阶段输出key value数据类型 也就是程序最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CovidCountBean.class);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        // 配置作业的输入数据路径
        FileInputFormat.addInputPath(job, input);
        // 配置作业的输出数据路径
        FileOutputFormat.setOutputPath(job, output);
        //判断输出路径是否存在 如果存在删除
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output)){
            fs.delete(output,true);
        }
        // 提交作业并等待执行完成
        boolean resultFlag = job.waitForCompletion(true);
        //程序退出
        System.exit(resultFlag ? 0 :1);
    }
}
