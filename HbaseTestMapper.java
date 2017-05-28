package retest;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HbaseTestMapper extends TableMapper<Text, IntWritable> {  
      
        @Override  
        protected void map(ImmutableBytesWritable key, Result value,  
                Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)  
                throws IOException, InterruptedException {  
            try {  
                for (Cell cell : value.listCells()) {  
                    String qualifier = new String(CellUtil.cloneQualifier(cell));  
                    String colValue = new String(CellUtil.cloneValue(cell), "UTF-8");  
                    System.out.println(qualifier + "=" + colValue + "\t");  
                    context.write(new Text(colValue), new IntWritable(1));  
                }  
            } catch (Exception e) {  
                e.printStackTrace();  
            }  
        }  
      
    }  