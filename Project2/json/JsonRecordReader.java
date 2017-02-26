package json;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * Json reader reads one json record at a time from text file, converting to
 * comma separated string.
 * 
 * @author wenlei
 * @author caitlin
 *
 */
public class JsonRecordReader extends RecordReader<LongWritable, Text> {

	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private LongWritable key = null;
	private Text value = null;
	private Seekable filePosition;

	private long backup;

	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {

		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);

		if (start == 0) {
			pos = start;
		} else {
			// if there were unprocessed lines in last split, backup to include
			start = split.getStart() - backup;
		}
		end = start + split.getLength();

		final Path file = split.getPath();

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());

		fileIn.seek(start);
		in = new LineReader(fileIn, job);
		filePosition = fileIn;

	}

	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);
		if (value == null) {
			value = new Text();
		}
		// tracks how much of the file left to read
		long newSize = 0;

		// We always read one extra line, which lies outside the upper
		// split limit i.e. (end - 1)
		while (pos <= end) {
			newSize = readJsonRecord(value, maxLineLength,
					Math.max(maxBytesToConsume(pos), maxLineLength));
			if (newSize == 0) {
				break;
			}
			pos += newSize;
			if (newSize < maxLineLength) {
				break;
			}
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	public long readJsonRecord(Text str, int maxLineLength, int maxBytesToConsume)
			throws IOException{

		// append all fields in record to a single line
		str.clear();

		// read opening brace
		long count = in.readLine(value, maxLineLength,
				Math.max(maxBytesToConsume(pos), maxLineLength));
		
		if (count == 0) {
			return count; // lineReader reached end of split
		}
		
		StringBuffer sb = new StringBuffer();
		String line = null;
		long next;
		// read lines until we reach end of split or record
		while (true) {
			next = in.readLine(value, maxLineLength,
					Math.max(maxBytesToConsume(pos), maxLineLength));
			if(next == 0){
				break;
			}
			else{
				count += next;
			}
			line = value.toString().trim().replaceAll("\"", "");

			if (line.equals("}") || line.equals("},")) { // end of record
				break;
			} 
			else {
				sb.append(line);
			}

		}
		if (line.equals("}") || line.equals("},")) { // end of record

			// save new text value
			str.set(sb.toString());
			return count;
			
		} else { // end of split can't read this record
			return 0;
		}
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress() throws IOException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void close() throws IOException {
		in.close();
	}

	private int maxBytesToConsume(long pos) {
		return (int) Math.min(Integer.MAX_VALUE, end - pos);
	}

}
