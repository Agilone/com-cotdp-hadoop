package com.cotdp.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;


public class TestRun2 {
	
	public static class MyMapper2 extends MapReduceBase
	implements Mapper<Text, BytesWritable, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable( 1 );
		private Text word = new Text();
		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub

		}
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub

		}
		@Override
		public void map(Text key, BytesWritable value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
						throws IOException {

			// NOTE: the filename is the *full* path within the ZIP file
			// e.g. "subdir1/subsubdir2/Ulysses-18.txt"
			String filename = key.toString();
			
			// We only want to process .txt files
			if ( filename.endsWith(".txt") == false )
				return;

			// Prepare the content 
			String content = new String( value.getBytes(), "UTF-8" );
			content = content.replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();

			// Tokenize the content
			StringTokenizer tokenizer = new StringTokenizer( content );
			while ( tokenizer.hasMoreTokens() )
			{
				word.set( tokenizer.nextToken() );
				output.collect( word, one );
			}
		}
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(TestRun2.class);
        
        
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
