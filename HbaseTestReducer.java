package retest;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import retest.DBStatic.ReStatic;

public class HbaseTestReducer extends Reducer<Text, IntWritable, ReStatic,NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, 
			Context context)
			throws IOException, InterruptedException {
		int i = 0;
		for (IntWritable val : values) {
			i += val.get();
		}
		String s = String.valueOf(i);
		System.out.println(key.toString() + "\t" + i);
		ReStatic res = new ReStatic();
		res.setCourt(key.toString());
		context.write(res,NullWritable.get());
	}

}
