package cn.itcast.hadoop.mr.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//求每个部门最早进入公司的员工姓名  
public class Test3 extends Configured implements Tool {
    private static String[] arrs;

    //MAP方法  
    public static class MapClass3 extends Mapper<LongWritable, Text, Text, Text> {
        private Map bumenmap = new HashMap();
        BufferedReader in = null;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            BufferedReader in = null;

            StringBuffer sb = new StringBuffer();
            File f = new File("E:\\hadoop\\in\\dept.txt");
            String line;
            if (f.exists()) {
                in = new BufferedReader(new InputStreamReader(
                        new FileInputStream(f)));
                while (null != (line = in.readLine())) {
                    String[] lins = line.split(",");
                    // 装进数组用于查询  
                    bumenmap.put(lins[0], lins[1]);
                }

            }

            if (null != in) {
                in.close();
            }
        }

        // 10,ACCOUNTING,NEW YORK  
        // 7499,ALLEN,SALESMAN,7698,20-2月-81,1600,300,30  
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            arrs = value.toString().split(",");
            if (arrs.length > 3) {
                if (bumenmap.containsKey(arrs[7])) {
                    if (null != arrs[4] && null != arrs[1]) {
                        //String gbkTime=new String(arrs[4].getBytes("UTF-8"),"gbk");  
                        //System.out.println(gbkTime);  
                        context.write(new Text(bumenmap.get(arrs[7]).toString()), new Text(new String(arrs[4]) + "," + arrs[1]));

                        System.out.println(arrs[7] + "-----" + new String(arrs[4].getBytes("gbk"), "UTF-8") + "," + arrs[1]);
                    }

                }
            }


        }
    }

    public static class Reducer3 extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws InterruptedException, IOException {
            // 员工姓名和进入公司日期
            String empName = null;
            String empEnterDate = null;


            // 设置日期转换格式和最早进入公司的员工、日期  
            DateFormat df = new SimpleDateFormat("dd-MM月-yy");

            Date earliestDate = new Date();

            String nowTime = df.format(earliestDate);

            String showTime = null;

            // 7499,ALLEN,SALESMAN,7698,20-2月-81,1600,300,30  
            //得到的数据是4和1  
            String[] reducerArrs = null;
            for (Text val : values) {
                reducerArrs = val.toString().split(",");//得到【20-2月-81，ALLEN】
                String oldTime = reducerArrs[0];
                // String newTime=new String(oldTime.getBytes("gbk"),"utf-8");  

                try {
                    Date yuangongtime = df.parse(reducerArrs[0]);//Sat Jan 23 00:00:00 CST 1982
                    Date nowtime2 = new Date();//Thu Mar 24 13:52:49 CST 2016

                    if (yuangongtime.before(nowtime2)) {
                        showTime = oldTime;
                        empName = reducerArrs[1];
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            context.write(new Text("公司部门为" + key + ":"), new Text("员工姓名是" + empName + "，入职时间是：" + showTime));
            System.out.println("公司部门为" + key + ":" + "员工姓名是" + empName + "，入职时间是：" + showTime);

        }


    }


    public int run(String[] strings) throws Exception {
        //实例化作业对象
        Job job = Job.getInstance();
        job.setJobName("Test3");
        job.setJarByClass(Test3.class);
        job.setMapperClass(MapClass3.class);
        job.setReducerClass(Reducer3.class);

        //设置输入格式
        job.setInputFormatClass(TextInputFormat.class);

        //设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //输入文件和输出文件位置设置
        File file = new File("E:\\hadoop\\test3out");

        delete(file);
        FileInputFormat.addInputPath(job, new Path("E:\\hadoop\\in"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop\\test3out"));


        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    private static void delete(File file) {

        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                delete(f);
                f.delete();
            }
        }
        file.delete();

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.setProperty("hadoop.home.dir",
                "E:\\hadjar\\hadoop-2.4.1-x64\\hadoop-2.4.1");
        int res = ToolRunner.run(new Configuration(), new Test3(), args);
        System.exit(res);
    }
}  