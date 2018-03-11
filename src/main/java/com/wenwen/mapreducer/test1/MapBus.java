package com.wenwen.mapreducer.test1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapBus extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, LongWritable> {


    @Override
    public void map(LongWritable key, Text date, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {

        //2013-01-11,-200
        String line = date.toString();
        if (line.contains(",")) {
            String[] tmp = line.split(",");
            String month = tmp[0].substring(5, 7);
            int money = Integer.valueOf(tmp[1]).intValue();
            output.collect(new Text(month), new LongWritable(money));
        }
    }
}