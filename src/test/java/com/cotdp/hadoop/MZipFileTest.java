package com.cotdp.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;


import com.cotdp.hadoop.MZipFileTest.MyMapper2.MyReducer2;





import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class MZipFileTest extends TestCase {

	private boolean isInitialised = false;
	private static final Log LOG = LogFactory.getLog(MZipFileTest.class);
	Configuration conf = new Configuration();
	Path workingPath = new Path("/tmp/" + this.getClass().getName());
	Path inputPath = new Path(workingPath + "/Input");


	public MZipFileTest( String testName )
	{
		super( testName );
	}

	/**
	 * Standard JUnit stuff
	 * @return
	 */
	public static Test suite()
	{
		return new TestSuite( MZipFileTest.class );
	}

	/**
	 * Prepare the FileSystem and copy test files
	 */
	@Override
	protected void setUp()
			throws Exception
			{
		// One-off initialisation
		if ( isInitialised == false )
		{
			LOG.info( "setUp() called, preparing FileSystem for tests" );

			// 
			FileSystem fs = FileSystem.get(conf);

			// Delete our working directory if it already exists
			LOG.info( "   ... Deleting " + workingPath.toString() );
			fs.delete(workingPath, true);

			// Copy the test files
			LOG.info( "   ... Copying files" );
			fs.mkdirs(inputPath);
			copyFile(fs, "zip-01.zip");
			copyFile(fs, "zip-02.zip");
			copyFile(fs, "zip-03.zip");
			copyFile(fs, "zip-04.dat");
			copyFile(fs, "random.dat");
			copyFile(fs, "encrypted.zip");
			copyFile(fs, "corrupt.zip");
			fs.close();

			//
			isInitialised = true;
		}
			}

	private void copyFile(FileSystem fs, String name)
			throws IOException
			{
		LOG.info( "copyFile: " + name );
		InputStream is = this.getClass().getResourceAsStream( "/" + name );
		OutputStream os = fs.create( new Path(inputPath, name), true );
		IOUtils.copyBytes( is, os, conf );
		os.close();
		is.close();
			}


	public static class MyMapper2 extends MapReduceBase
	implements Mapper<Text, Text, Text, Text>
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
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {

			// NOTE: the filename is the *full* path within the ZIP file
			// e.g. "subdir1/subsubdir2/Ulysses-18.txt"
			String filename = key.toString();
			LOG.info( "map: " + filename );

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
				output.collect( null, word );
			}
		}


		public static class MyReducer2 extends MapReduceBase
		implements Reducer<Text, IntWritable, Text, IntWritable>
		{

			@Override
			public void configure(JobConf job) {
				// TODO Auto-generated method stub

			}

			@Override
			public void close() throws IOException {
				// TODO Auto-generated method stub

			}

			@Override
			public void reduce(Text key, Iterator<IntWritable> values,
					OutputCollector<Text, IntWritable> output, Reporter reporter)
							throws IOException {
				int sum = 0;
				IntWritable val;
				while ( values.hasNext() )
				{
					val = values.next();
					sum += val.get();
				}
				output.collect(key, new IntWritable(sum));
			}

		}
	}
	
	
	
	public void testSingle()
	        throws IOException, ClassNotFoundException, InterruptedException
	 
	    {
	        LOG.info( "============================================================" );
	        LOG.info( "==                Running testSingle()                    ==" );
	        LOG.info( "============================================================" );
	        
	        // Standard stuff
	        JobConf conf = new JobConf(MZipFileTest.class);
	        
	        
	        conf.setJobName(this.getClass().getSimpleName());
	        conf.setJarByClass(this.getClass());
	        conf.setMapperClass(MyMapper2.class);
	        conf.setReducerClass(MyReducer2.class);
	       
	        // 
	        conf.setInputFormat(MZipFileInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        
	        // The output files will contain "Word [TAB] Count"
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(IntWritable.class);
	        
	        //
	        MZipFileInputFormat.setInputPaths(conf, new Path(inputPath, "zip-01.zip"));
	        TextOutputFormat.setOutputPath(conf, new Path(workingPath, "Output_Single"));
	        
	        //
	        RunningJob job = JobClient.runJob(conf);
	        job.waitForCompletion();
	        assertTrue( job.isSuccessful() );
	    }
}
