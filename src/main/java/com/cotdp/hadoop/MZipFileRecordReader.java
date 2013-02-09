package com.cotdp.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;


public class MZipFileRecordReader implements RecordReader<NullWritable, Text> {
	
	private FSDataInputStream fsin;
	private ZipInputStream zip;
	private boolean isFinished = false;
	private long position = 0;
	private ZipEntry entry = null;
	
	public MZipFileRecordReader(InputSplit inputSplit,
			JobConf job, Reporter reporter) throws IOException {
		FileSplit split = (FileSplit) inputSplit;

	    Path path = split.getPath();
	    try {
			FileSystem fs = path.getFileSystem( job );
			fsin = fs.open( path );
			zip = new ZipInputStream( fsin );
			entry = zip.getNextEntry();
		} catch (IOException e) {
			throw e;
		}
		
	}

	@Override
	public boolean next(NullWritable key, Text value) throws IOException {
		
		if ( entry == null )
	    {
	      isFinished = true;
	      return false;
	    }
		
		//key.set(new Text(entry.getName()));
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    byte[] temp = new byte[10485760];

	    int bytesRead = 0;
	    try
	    {
	      bytesRead = zip.read( temp, 0, 10485760 );
	    }
	    catch ( EOFException e )
	    {
	      if ( ZipFileInputFormat.getLenient() == false )
	        throw e;
	      entry = null;
	      return false;
	    }
	    
	    position += bytesRead;
	    
	    if ( bytesRead > 0 )
	      bos.write( temp, 0, bytesRead );
	    else
	        entry = zip.getNextEntry();
	    
	    
	    
	    
	    value.set(new String(bos.toByteArray()));
		
	    return true;
	}

	@Override
	public NullWritable createKey() {
		return NullWritable.get();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		return position;
	}

	@Override
	public void close() throws IOException {
		try { zip.close(); } catch(Exception e) {}
		try { fsin.close(); } catch(Exception e) {}
	}

	@Override
	public float getProgress() throws IOException {
		return isFinished ? 1 : 0;
	}

}
