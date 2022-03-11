package com.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduce {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 创建配置
        Configuration configuration = new Configuration();

        // 清理已经存在的输出目录
        Path out = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
            System.out.println("output exists, but it has deleted");
        }

        // 创建job
        Job job = Job.getInstance(configuration, "MyMapReduce");
        //设置job的处理类
        job.setJarByClass(MapReduce.class);
        job.setNumReduceTasks(1);

        //设置map相关的参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flow.class);

        //设置reduce相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.out.println(args[0]);
        System.out.println(args[1]);

        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
