package com.cotdp.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class MZipFileInputFormat extends FileInputFormat<Text, BytesWritable> {

	@Override
	public RecordReader<Text, BytesWritable> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		return new MZipFileRecordReader(split,
				job, reporter);
	}
	
	protected boolean isSplitable(FileSystem fs, Path filename) {
	    return false;
	  }

}