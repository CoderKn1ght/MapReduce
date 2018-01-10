package averageSalary;

import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
*
* @author Shashank, Sarbajit
*/
public class AverageSalary {

//mapper
public static class SalaryMapper extends Mapper<LongWritable,Text,Text,Text>{
@Override
	public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException{
		String str = value.toString();
		//Splitting the line with "," since it is a CSV.
		String[] strList = str.split(",");
		//Skipping the first line of the csv
		if(!strList[0].equals("serialno")){
			//Fetching the value of the state, gender and wage.
			String state = strList[5];
			String gender = strList[69];
			String wage = strList[72];
			//remove the additional 0's at the start of the wage
			wage = wage.replaceFirst ("^0*", "");
			//Checking if the wage is a number or not
			if(wage.matches("^[0-9]+$")){
				c.write(new Text(state + "_"+ gender),new Text(wage));
			}
		}
			
	}
}
//reducer
public static class SalaryReducer extends Reducer<Text,Text,Text,Text>{
	@Override
	public void reduce(Text key, Iterable<Text>values, Context c) throws IOException,InterruptedException{
		int count = 0;
		Double totalSalary = 0.0;
//For each value obtained from the mapper, computing the total salary and number of people
		for(Text val:values){
			totalSalary += Double.parseDouble(val.toString());
			count += 1;
		}
		//Calculating the Average Salary based on the total salary and count.
		Double averageSalary = totalSalary/count;
		//Formatting the average salary to give only first 2 decimal points.
		DecimalFormat df = new DecimalFormat("#.00");
		c.write(key, new Text(""+df.format(averageSalary)));
		}
	}

public static void main(String[] args) throws IOException, ClassNotFoundException, 
InterruptedException{
	Configuration conf = new Configuration();
	Job j2 = new Job(conf);
	j2.setJobName("Salary Average job");
	j2.setJarByClass(AverageSalary.class);
	//Mapper input and output
	j2.setMapOutputKeyClass(Text.class);
	j2.setMapOutputValueClass(Text.class);
	//Reducer input and output
	j2.setOutputKeyClass(Text.class);
	j2.setOutputValueClass(Text.class);
	//file input and output of the whole program
	j2.setInputFormatClass(TextInputFormat.class);
	j2.setOutputFormatClass(TextOutputFormat.class);
	//Set the mapper class 
	j2.setMapperClass(SalaryMapper.class);
	//set the combiner class for custom combiner
	//j2.setCombinerClass(SalaryReducer.class);
	//Set the reducer class
	j2.setReducerClass(SalaryReducer.class);
	//set the number of reducer
	//if it is zero means there is no reducer
	//j2.setNumReduceTasks(2);
	FileOutputFormat.setOutputPath(j2, new Path(args[1]));
	FileInputFormat.addInputPath(j2, new Path(args[0]));
	j2.waitForCompletion(true);
	}
}

