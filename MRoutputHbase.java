import java.io.File;
import java.io.FileReader;
import java.io.IOException;  
import java.io.LineNumberReader;
import java.util.HashMap;  
import java.util.Iterator;  
import java.util.Map;  
import java.util.Map.Entry;  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;  
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;  
import org.apache.hadoop.hbase.util.Bytes;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  

 

public class MRoutputHbase {  
	private static final String TABLE_NAME = "weather";	
    private static final String columnfamily="cf";
    private static HTable htable;
  
    public static class BulkLoadMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {    
	    private String hbaseTable;
	    
	    private String columnFamily1;
	    	
	    private ImmutableBytesWritable hbaseTableName;		
	    
	    public void setup(Context context) {
	        Configuration configuration = context.getConfiguration();		
	        hbaseTable = configuration.get("hbase.table.name");		
	       		
	        columnFamily1 = configuration.get("columnfamily");		
	        		
	        hbaseTableName = new ImmutableBytesWritable(Bytes.toBytes(hbaseTable));		
	    }

           @SuppressWarnings("deprecation")
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
        	   
        	   
        	   try {		
                   String[] values = value.toString().split("\t");	
		//get line number for generating keys	
         LineNumberReader  lnr = new LineNumberReader(new FileReader(new File("hdfs://localhost:54310/user/hduser/Accidents/Output")));
		 lnr.skip(Long.MAX_VALUE);
		 int count = lnr.getLineNumber() + 1;
		 // Finally, the LineNumberReader object should be closed to prevent resource leak
		 lnr.close();
        for(int i=0;i<count;i++)
		{
                   
                   
                   Put put = new Put(Bytes.toBytes("row"+i));			
                  			
                   put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("Weather"), Bytes.toBytes(values[1]));
		   put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("Accidents"), Bytes.toBytes(values[1]));			
                   
                   context.write(hbaseTableName, put);
                   htable.put(put);
		}
               } catch(Exception exception) {			
                   exception.printStackTrace();	
               }
         }  
	      @SuppressWarnings("deprecation")
		public static void main(String[] args) throws Exception {  
	           Configuration configuration = HBaseConfiguration.create();  
	           configuration.set("fs.defaultFS", "hdfs://localhost:54310");
	   		   configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );
	   		 configuration.set("hbase.table.name",TABLE_NAME);		
	         configuration.set("columnfamily",columnfamily);	
	           String inputPath = "hdfs://localhost:54310/user/hduser/Accidents/Output";  
	           String outputPath = "hdfs://localhost:54310/user/hduser/Hbase/Output"; 
	           htable = new HTable(configuration, "weather");  
	           @SuppressWarnings("deprecation")
			   Job job = new Job(configuration,"HBase  save");        
	           job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
	           job.setMapOutputValueClass(Put.class);  
	           job.setInputFormatClass(TextInputFormat.class);  
	           job.setOutputFormatClass(HFileOutputFormat.class);  
	           job.setJarByClass(MRoutputHbase.class);    
	           FileInputFormat.setInputPaths(job, new Path(inputPath));  
	           FileOutputFormat.setOutputPath(job,new Path(outputPath));             
	           HFileOutputFormat.configureIncrementalLoad(job, htable);  
	           System.exit(job.waitForCompletion(true) ? 0 : 1);  
	      }  
    }  
 }