package com.cotdp.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * TestRun given a Zip File (arg 0 ) unzips into location in arg 1.
 */
public class TestRun {







  public static  void main(String args[])
  {

    Configuration conf = new Configuration();
    conf.set("mapred.job.reduce.memory.mb", "2048");
    // Standard stuff
    Job job = null;
    try {
      job = new Job(conf);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    job.setJobName("TestRun");
    job.setJarByClass(MyMapper.class);

    job.setMapperClass(MyMapper.class);

    //
    job.setInputFormatClass(ZipFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    //
    //ZipFileInputFormat.setInputPaths(job, new Path(inputPath, "zip-01.zip"));
    try {
      ZipFileInputFormat.setInputPaths(job,new Path(args[0]));
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    try {
      TextOutputFormat.setOutputPath(job, new Path(args[1]));
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }


    //
    try {
      job.waitForCompletion(true);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (ClassNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

//  public static class Map extends MapReduceBase implements Mapper {
//
//
////    @Override
////    public void map(LongWritable key, Text value, OutputCollector output, Reporter reporter) throws IOException {
////
////        output.collect(key,value);
////
////      }
//
//    @Override
//    public void map(Object o, Object o1, OutputCollector outputCollector, Reporter reporter) throws IOException {
//
//      outputCollector.collect(o,o1);
//
//    }
//  }
//
//
//  public static void main(String[] args) throws Exception {
//    JobConf conf = new JobConf(Map.class);
//    conf.setJobName("test");
//    conf.setOutputKeyClass(Text.class);
//    conf.setOutputValueClass(BytesWritable.class);
//    conf.setInputFormat((Class<? extends InputFormat>) ZipFileInputFormat.class);
//    conf.setOutputFormat((Class<? extends OutputFormat>) TextOutputFormat.class);
//    conf.setMapperClass((Class<? extends org.apache.hadoop.mapred.Mapper>) Map.class);
//
//
//    FileInputFormat.setInputPaths(conf, new Path(args[1]));
//    FileOutputFormat.setOutputPath(conf, new Path(args[2]));
//    JobClient.runJob(conf);
//  }
}
