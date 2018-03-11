package com.wenwen.mapreducer.test1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Wallet {

    private static String checkHadoopHome() {

        // first check the Dflag hadoop.home.dir with JVM scope
        String home = System.getProperty("hadoop.home.dir");

        // fall back to the system/user-global env variable
        if (home == null) {
            home = System.getenv("HADOOP_HOME");
        }

        if(home != null){
            System.setProperty("hadoop.home.dir", home);
        } else {
            System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-2.7.3");
        }

        return home;
    }
    public static void main(String[] args) {

        // hdfs://192.168.131.129:9000/input/ hdfs://192.168.131.129:9000/output3

        if (args.length != 2) {
            System.err.println("param error!");
            System.exit(-1);
        }

        if(checkHadoopHome() != null){
            return;
        }


        JobConf jobConf = new JobConf(Wallet.class);
        jobConf.setJobName("My Wallet");

        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
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