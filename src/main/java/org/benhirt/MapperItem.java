package org.benhirt;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperItem extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split(",");
        System.out.println(lines[2] +" "+lines[lines.length-1]);
        context.write(new Text(lines[2]), new DoubleWritable(Double.parseDouble(lines[lines.length-1])) );
    }
}
