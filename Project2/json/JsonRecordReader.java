

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * Json reader reads one json record at a time from text file, converting to
 * comma separated string.
 * 
 * @author wenlei
 * @author caitlin
 *
 */
public class JsonRecordReader extends RecordReader<LongWritable, Text> {

	private LineRecordReader in;
	private LongWritable key = null;
	private Text value = null;

	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {

		in = new LineRecordReader();
		in.initialize(genericSplit, context);
		key = new LongWritable();
		value = new Text();
	}

	@Override
	public void close() throws IOException {
		in.close();
	}

	@Override
	public float getProgress() throws IOException {
		return in.getProgress();
	}

	@Override
	/**
	 * Read multiple lines from file into new json record.
	 * @return success
	 */
	public boolean nextKeyValue() throws IOException, InterruptedException {

		// append all fields in record to a single line
		StringBuffer sb = new StringBuffer();
		String line = null;
		// read opening brace
		if (!in.nextKeyValue()) {
			return false; // lineReader reached end of split
		}

		// read lines until we reach end of split or record
		while (in.nextKeyValue()) {
				line = in.getCurrentValue().toString().trim().replaceAll("\"", "");
				
				if (line.equals("}") || line.equals("},")){ //end of record
					break;
				}
				else{
					sb.append(line);
				}

		}
		if (line.equals("}") || line.equals("},")) {  //end of record

			// save new text value
			this.value.set(sb.toString());
			// save new key value
			this.key = in.getCurrentKey();
			return true;
		} 
		else {
			return false; //end of split can't read this record
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return this.key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}
}
