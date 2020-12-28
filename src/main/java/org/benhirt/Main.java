package org.benhirt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;



public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        String[] arguments = new GenericOptionsParser(configuration,args).getRemainingArgs();
        Job job = Job.getInstance(configuration, "Sales");
        job.setJarByClass(Main.class);

        if(arguments[2].equals("country")){
            job.setMapperClass(MapperCountry.class);
            job.setReducerClass(ReducerTotal.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
        }
        else if(arguments[2].equals("item")){
            job.setMapperClass(MapperItem.class);
            job.setReducerClass(ReducerTotal.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

        }else if (arguments[2].equals("region")){
            job.setMapperClass(MapperRegion.class);
            job.setReducerClass(ReducerTotal.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
        }
        else {
            job.setMapperClass(MapperSales.class);
            job.setReducerClass(ReducerOnlineOffline.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }
        FileInputFormat.addInputPath(job, new Path(arguments[0]));
        FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
        if(job.waitForCompletion(true)){
            System.out.println("Traitement terminé");
            System.exit(0);
        }
        System.exit(-1);

    }

}
