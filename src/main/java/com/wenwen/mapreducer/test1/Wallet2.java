package com.wenwen.mapreducer.test1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class Wallet2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("df.default.name", "hdfs://60.205.225.167:9000");
        conf.set("hadoop.job.user","hadoop");
        Path in = new Path("hdfs://60.205.225.167:9000/wenwen/test1_2013-01.txt");
        Path out = new Path("hdfs://60.205.225.167:9000/wenwen/output/");

        try {
            out.getFileSystem(conf).delete(out, true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        JobConf jobConf = new JobConf(Wallet2.class);
        jobConf.setJobName("My Wallet2");

        FileInputFormat.addInputPath(jobConf, in);
        FileOutputFormat.setOutputPath(jobConf, out);
        jobConf.setMapperClass(MapBus.class);
        jobConf.setReducerClass(ReduceBus.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(LongWritable.class);

        try {
            JobClient.runJob(jobConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}  