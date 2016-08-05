import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//The Map output is input to Reducer and Reducer output key and output value 
public class WeatherReducer extends
       Reducer<Text, IntWritable, Text, IntWritable>{
	
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	       throws IOException, InterruptedException {
//Iterate through the array and counting the sum of the Map output
		int sum = 0;
		int count=0;
		 for (IntWritable val : values)
		 {
			sum += val.get();           
			count++;
		 }	
		 
		 //lets make the total of that field 
		 //sum=sum/count;
		 
// Passing the output key and sum of the value as output value
		context.write(key, new IntWritable(sum));
		
		
	}
	
	//protected void cleanup(Context context) throws IOException, InterruptedException {
		//context.write(new Text(Integer.toString(counter)), NullWritable.get());
}