package ageHistogram;

import java.io.IOException;
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
public class AgeHistogram {

//mapper
public static class AgeMapper extends Mapper<LongWritable,Text,Text,Text>{
@Override
	public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException{
		String str = value.toString();
		//Splitting the line with "," since it is a CSV.
		String[] strList = str.split(",");
		//Skipping the first line
		if(!strList[0].equals("serialno")){
			//Fetching the values of age and year
			int age = Integer.parseInt(strList[8]);
			int year = Integer.parseInt(strList[0].substring(0,4));
			//Distributing the ages into their respective buckets.
			if(age>=0 && age<10){
				c.write(new Text(year+"_0-9"),new Text("1"));
			} else if(age>=10 && age<20){
				c.write(new Text(year+"_10-19"),new Text("1"));
			} else if(age>=20 && age<30){
				c.write(new Text(year+"_20-29"),new Text("1"));
			} else if(age>=30 && age<40){
				c.write(new Text(year+"_30-39"),new Text("1"));
			} else if(age>=40 && age<50){
				c.write(new Text(year+"_40-49"),new Text("1"));
			} else if(age>=50 && age<60){
				c.write(new Text(year+"_50-59"),new Text("1"));
			} else if(age>=60 && age<70){
				c.write(new Text(year+"_60-69"),new Text("1"));
			} else if(age>=70 && age<80){
				c.write(new Text(year+"_70-79"),new Text("1"));
			} else if(age>=80 && age<90){
				c.write(new Text(year+"_80-89"),new Text("1"));
			} else if(age>=90 && age<100){
				c.write(new Text(year+"_90-99"),new Text("1"));
			}
		}
	}
}
//reducer
public static class AgeReducer extends Reducer<Text,Text,Text,Text>{
	@Override
	public void reduce(Text key, Iterable<Text>values, Context c) throws IOException,InterruptedException{
		int count = 0;
		//For each value, counting the number of people in that bucket
		for(Text val:values){
			count += 1;
		}
		c.write(key, new Text(""+count));
		}
	}

public static void main(String[] args) throws IOException, ClassNotFoundException, 
InterruptedException{
	Configuration conf = new Configuration();
	Job j2 = new Job(conf);
	j2.setJobName("Age Count per year");
	j2.setJarByClass(AgeHistogram.class);
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
	j2.setMapperClass(AgeMapper.class);
	//Set the reducer class
	j2.setReducerClass(AgeReducer.class);
	FileOutputFormat.setOutputPath(j2, new Path(args[1]));
	FileInputFormat.addInputPath(j2, new Path(args[0]));
	j2.waitForCompletion(true);
	}
}

