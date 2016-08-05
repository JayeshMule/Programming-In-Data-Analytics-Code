import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class AvgSpeedDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:54310");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		Job job = Job.getInstance(conf, "Group By 3  and avg of Field 6");
		//Job job = Job.getInstance();
		job.setJarByClass(AvgSpeedDriver.class);
		job.setMapperClass(AvgSpeedMapper.class);
		job.setReducerClass(AvgSpeedReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/hduser/Accidents/part-m-00000"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/hduser/Accidents/AvgOutput"));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}