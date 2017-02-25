package json;
import java.io.IOException;

import json.JsonRecordReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Jason input format parses json files. It reads a ingle json record as a line
 * and passes it to the map or reduce function as a string of commas separated
 * values.
 * 
 * @author wenlei
 * @author caitlin
 *
 */
public class JsonInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		JsonRecordReader jsonreader = new JsonRecordReader();
		jsonreader.initialize(split, context);
		return jsonreader;
	}

}