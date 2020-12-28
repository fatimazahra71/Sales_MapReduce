package org.benhirt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ReducerTotal extends Reducer<Text, IntWritable, Text, Text> {
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> i=values.iterator();
        double profit =0;
        while(i.hasNext()) {
            profit+=i.next().get();
        }
        context.write(key, new Text(" Le profit total :  "+profit) );

    }
}
