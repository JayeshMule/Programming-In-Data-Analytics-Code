import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Setting Map input key,input value and output key and output value
public class AvgSpeedMapper extends Mapper<Object, Text, Text, IntWritable>
{
	//Creating object of IntWritable and Text
	private IntWritable accidents  = new IntWritable();
	private Text  speed = new Text();
   //Passing Map input key and value and context to hold the Map output key and value 
			public void map(Object key, Text value, Context context)
					throws IOException, InterruptedException
					{
	//Passing the string and splitting it by comma separated and storing it in string array
				String[] arr_new = value.toString().split(",");
    //Inserting the value at the 2nd position of the string array
				speed.set(arr_new[14]);
	//Inserting the value at the 4th position of the string array and parsing it to integer
				accidents.set(Integer.parseInt(arr_new[6]));
	//Passing the key and value to context buffer of the Map output 
				context.write(speed, accidents);
				
				
		}
		
}
	