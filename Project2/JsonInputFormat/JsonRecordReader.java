package Q2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;



public class JsonRecordReader extends RecordReader<LongWritable, Text>{
	
	private long start;
	private long end;
	private String start_Tag = null;
	private FSDataInputStream fsin;
	private final DataOutputBuffer outputBuffer = new DataOutputBuffer();
	private byte[] startbyte = null;
	
	private Text value = null;
	private LongWritable key = null;
	

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		value.set(outputBuffer.getData(), 0, outputBuffer.getLength());
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void initialize(InputSplit inputsplit, TaskAttemptContext context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		start_Tag = conf.get("start_tag");
		startbyte = start_Tag.getBytes("utf-8");
		FileSplit split = (FileSplit)inputsplit;
		start = split.getStart();
		end = start + split.getLength();
		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(conf);
		fsin = fs.open(file);
		fsin.seek(start);
	}
	
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		key = new LongWritable();
		value = new Text();
		outputBuffer.reset();
		
		if(null != start_Tag && fsin.getPos() < end){
			int i = 0;
			while(true){
				int read = fsin.read();
				if(read == -1){
					return true;
				}
				if(read == startbyte[i]){
					i++;
					if(i >= startbyte.length){
						boolean start = false;
						while(true){
							int datawrite = fsin.read();
							if(datawrite == JsonByteConstants.JSON_COLON){
								start = true;
								int counter = 0;
								boolean comma = true;
								key.set(fsin.getPos());
								while(start){
									datawrite = fsin.read();
									
									if(datawrite == JsonByteConstants.JSON_START_BRACE || 
											datawrite == JsonByteConstants.JSON_ARRAY_START){
										counter++;
										comma = false;
									}
									else if(datawrite == JsonByteConstants.JSON_END_BRACE ||
											datawrite == JsonByteConstants.JSON_ARRAY_END){
										counter--;
										if(counter == 0){
											start = false;
											comma = true;
											outputBuffer.write(datawrite);
											return true;
										}
										else if(counter < 0){
											//break;
											return true;
										}
									}
									else if(datawrite == JsonByteConstants.JSON_COMMA && comma){
										//break;
										return true;
									}
									if(start)
										outputBuffer.write(datawrite);
								}
								/*i = 0;
								break;*/
							}
						}
					}
				}
				else{
					i = 0;
				}
				
			}
		}
		return false;
	}

}