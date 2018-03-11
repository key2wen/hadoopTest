package com.wenwen.mapreducer.test1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ReduceBus extends MapReduceBase
        implements Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    public void reduce(Text month, Iterator<LongWritable> money,
                       OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {
        int total_money = 0;
        while (money.hasNext()) {
            total_money += money.next().get();
        }
        output.collect(month, new LongWritable(total_money));
    }
}  