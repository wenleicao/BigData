
import java.io.IOException;



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
	private static final long MAX_SPLIT_SIZE = 1522428;  // airfield size's 1/5  204800,  full size 1,022,428
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		JsonRecordReader jsonreader = new JsonRecordReader();
		jsonreader.initialize(split, context);
		return jsonreader;
	}
	
	@Override
	 protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
	  return super.computeSplitSize(blockSize, minSize, MAX_SPLIT_SIZE);
	 }
		

}