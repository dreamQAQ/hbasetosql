package retest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBStatic {
	public static class ReStatic implements Writable, DBWritable {
		
	    private String court;
		

		public ReStatic(){}
	 
	     @Override
	     public void write(PreparedStatement statement) throws SQLException {
	         statement.setBytes(1, Bytes.toBytes(this.getCourt()));
	     }
	 
	     @Override
	     public void readFields(ResultSet resultSet) throws SQLException {
	         this.court = resultSet.getString(1);	      
	     }
	     
	     public String getCourt() {
				return court;
			}

		 public void setCourt(String court) {
				this.court = court;
			}
			
	     @Override
	     public void write(DataOutput out) throws IOException {
	         out.writeUTF(this.court);
	     }
	 
	     @Override
	     public void readFields(DataInput in) throws IOException {
	         this.court = in.readUTF();
	     }
 

	 }

}
