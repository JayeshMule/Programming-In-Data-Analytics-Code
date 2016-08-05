import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//The Map output is input to Reducer and Reducer output key and output value 
public class AvgSpeedReducer extends
       Reducer<Text, IntWritable, Text, IntWritable>{
	
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	       throws IOException, InterruptedException {
//Iterate through the array and count the average speed  of the Map output speed
		int sum = 0;
		int count=0;
		int avgspeed;
		 for (IntWritable val : values)
		 {
			sum += val.get();           
			count++;
		 }	
		 //count average speed 
		 avgspeed=sum/count;
		 
// Passing the output key and sum of the value as output value
		context.write(key, new IntWritable(avgspeed));
		
		
	}
	
	//protected void cleanup(Context context) throws IOException, InterruptedException {
		//context.write(new Text(Integer.toString(counter)), NullWritable.get());
}