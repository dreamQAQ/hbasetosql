package retest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;

import retest.DBStatic.ReStatic;

public class HbaseTest {
    public static void main(String[] args) throws ClassNotFoundException, InterruptedException {  
//    	时间,原被告，法院，案件字号，审判长，代理审判员，书记员
    	String family = "baseinfo";
        String column1 = "法院";
        String tableName = "lawcase";
        Configuration conf = HBaseConfiguration.create();  
        DBConfiguration.configureDB(
                conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/test",
                "root","123");
         
//        Configuration config = new Configuration() ;
        
        try {  
        	Job job = Job.getInstance(conf, "analyze table data for " + tableName);
            job.setJarByClass(HbaseTest.class);  
            Scan scan = new Scan();   
            scan.setCaching(500);   
            scan.setCacheBlocks(false);  
            scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column1));
            TableMapReduceUtil.initTableMapperJob(Bytes.toBytes(tableName), scan, HbaseTestMapper.class, Text.class,  
                    IntWritable.class, job);  
            job.setReducerClass(HbaseTestReducer.class);  
            job.setNumReduceTasks(1);
            DBOutputFormat.setOutput(
                    job,                // job
                    "test",       // output table name
                    "court"       // fields
            ); 
            job.setMapperClass(HbaseTestMapper.class);             
            job.setCombinerClass(HbaseTestCombine.class);                  
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputFormatClass(DBOutputFormat.class);
            job.setOutputKeyClass(ReStatic.class);
            job.setOutputValueClass(NullWritable.class);
            System.exit(job.waitForCompletion(true) ? 0 : 1);  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }  
}