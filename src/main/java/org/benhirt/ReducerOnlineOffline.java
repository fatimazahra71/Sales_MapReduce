package org.benhirt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ReducerOnlineOffline extends Reducer<Text, IntWritable, Text, Text> {

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> i=values.iterator();
        double offlineProfit =0;
        double onlineProfit =0;
        while(i.hasNext()) {
            String[] temp = i.next().toString().split(",");
            if(temp[0].equals("Online")) onlineProfit+=Double.parseDouble(temp[1]);
            else offlineProfit+=Double.parseDouble(temp[1]);
        }
        context.write(key, new Text("  Offline Sales = "+offlineProfit +"\n Online Sales ="+onlineProfit) );

    }
}
