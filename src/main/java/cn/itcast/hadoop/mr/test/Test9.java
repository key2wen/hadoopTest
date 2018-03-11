package cn.itcast.hadoop.mr.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
import org.jboss.netty.util.internal.StringUtil;

//将全体员工按照总收入（工资+提成）从高到低排列  
public class Test9 extends Configured implements Tool {


    public static class Map9 extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] arrsMap = value.toString().split(",");
            if (arrsMap.length > 5) {
                //7934,MILLER,CLERK,7782,23-1月-82,1300,,10  
                if (arrsMap[6].length() > 1) {
                    long allSala = (Long.parseLong(arrsMap[5]) + Long.parseLong(arrsMap[6]));
                    context.write(new Text("0"), new Text(arrsMap[1] + "," + allSala + ""));
                } else {
                    context.write(new Text("0"), new Text(arrsMap[1] + "," + arrsMap[5]));
                }

            }
        }
    }

    public static class Reducer9 extends Reducer<Text, Text, Text, Text> {
        static int reducernum = 0;
        static long max = 0;
        static long num2 = 0;
        static long num3 = 0;
        //设置一个map存这些数据  
        Map<String, Long> showmap = new HashMap<String, Long>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            // 定义工资前三员工姓名
            String empName;


            // 定义工资前三工资  
            int empSalary = 0;


            List<Integer> showlist = new ArrayList<Integer>();

            Map namemap = new HashMap();
            // 通过冒泡法遍历所有员工，比较员工工资多少，求出前三名  
            for (Text val : values) {
                empName = val.toString().split(",")[0];
                empSalary = Integer.parseInt(val.toString().split(",")[1].toString());
                namemap.put(empSalary, empName);
                showlist.add(empSalary);

            }

            int[] arrs = new int[showlist.size()];
            for (int i = 0; i < showlist.size(); i++) {
                arrs[i] = showlist.get(i);
            }
            toarryal(arrs);
            for (int i = 0; i < arrs.length; i++) {
                String name = (String) namemap.get(arrs[i]);

                context.write(new Text(name), new Text(arrs[i] + ""));
                System.out.println("姓名是:" + name + "----" + "工资是" + arrs[i]);
            }  
                
              
          /*  for (int i = 0; i < showlist.size(); i++) {  //[ 10,20,5] 
                for (int j = 1; j < showlist.size(); j++) { 
                    if (Integer.parseInt((String) showlist.get(i))<Integer.parseInt((String) showlist.get(j))) { 
                        showlist 
                    } 
                    if (showlist.get(i)<showlist.get(j)) { 
                        showlist. 
                    } 
                } 
            }*/


            // 输出工资前三名信息  
         /*   context.write(new Text( "First employee name:" + firstEmpName), new Text("Salary:"          + firstEmpSalary)); 
            System.out.println( "First employee name:" + firstEmpName + "---"+firstEmpSalary); 
            context.write(new Text( "Second employee name:" + secondEmpName), new Text("Salary:" + secondEmpSalary)); 
            System.out.println( "First employee name:" + secondEmpName + "---"+secondEmpSalary); 
            context.write(new Text( "Third employee name:" + thirdEmpName), new Text("Salary:"          + thirdEmpSalary)); 
            System.out.println( "First employee name:" + thirdEmpName + "---"+thirdEmpSalary); 
               */  
              /* for (int i = 0; i < showlist.size(); i++) { 
                String value=   showlist.get(i).toString(); 
                String[] arrs=value.split("。"); 
                  context.write(new Text(arrs[0]),new Text(arrs[1])); 
                  System.out.println(arrs[0]+"------"+arrs[1]); 
            }*/


        }


        private void toarryal(int[] arrs) {
            int temp = 0;
            for (int i = 0; i < arrs.length; i++) {
                for (int j = i + 1; j < arrs.length; j++) {
                    if (arrs[i] < arrs[j]) {
                        temp = arrs[i];
                        arrs[i] = arrs[j];
                        arrs[j] = temp;

                    }
                }
            }

        }


    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.setProperty("hadoop.home.dir",
                "E:\\hadjar\\hadoop-2.4.1-x64\\hadoop-2.4.1");
        int res = ToolRunner.run(new Configuration(), new Test9(), args);
        System.exit(res);
    }

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("test8");
        job.setJarByClass(Test9.class);
        job.setMapperClass(Map9.class);
        job.setReducerClass(Reducer9.class);

        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);

        // 设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
        File file = new File("E:\\hadjar\\test9out");
        delete(file);
        FileInputFormat.addInputPath(job, new Path("E:\\hadjar\\in"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadjar\\test9out"));

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
}  