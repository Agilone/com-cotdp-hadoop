package com.cotdp.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.cotdp.hadoop.MZipFileTest.MyMapper2;

public class TestRun2 {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(MZipFileTest.class);
        
        
        conf.setJobName("asd");
        conf.setJarByClass(MyMapper2.class);
        conf.setMapperClass(MyMapper2.class);
        
        // 
        conf.setInputFormat(MZipFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        // The output files will contain "Word [TAB] Count"
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setNumReduceTasks(0);
        //
        MZipFileInputFormat.setInputPaths( conf,new Path(args[0]));
        TextOutputFormat.setOutputPath(conf,new Path(args[1]));
        
        //
        RunningJob job = JobClient.runJob(conf);
        job.waitForCompletion();
        
	}

}
