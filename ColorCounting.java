import java.awt.image.ColorModel;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Random;

import javax.sound.midi.SysexMessage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ColorCounting {

	public static HashMap<Integer, Integer> colorMap = null; 

	//Mapper input is <Object, Text> and output is <IntWritable,Text>.
	public static class Mapper1 extends
			Mapper<Object, Text, IntWritable, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String eachLine = value.toString();
			String[] edge = eachLine.split(" ");
			
			Integer v1,v2;
			v1 = Integer.parseInt(edge[0]);
			v2 = Integer.parseInt(edge[1]);
			Text out_edge = new Text();
			
			//If the edge is a monochromatic edge.
			if(colorMap.get(v1) == colorMap.get(v2))
			{
				IntWritable color = new IntWritable(colorMap.get(v1));
				out_edge.set(edge[0]+" "+edge[1]);
				
				context.write(color, out_edge);
			}
			//else do nothing.

		}
	}

	/**
	 ** The reducer class of selecting monochromatic edges
	 *	Input : <color, edge> (int, text)
	 * */
	public static class Reducer1 extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> edges,
				Context context) throws IOException, InterruptedException {
			for (Text e : edges) {
				context.write(key, e);
			}
		}
	}

	public static int randInt(int min, int max, Random rand) {

	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum = rand.nextInt((max - min) + 1) + min;

	    return randomNum;
	}	

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

	    Random rand = new Random();	//For generating random colors 
	    //HashMap<Integer, Integer> colorMap = new HashMap<Integer, Integer>();
	    colorMap = new HashMap<Integer, Integer>();
		BufferedReader br = new BufferedReader(new FileReader(
				"/home/kiriti/acads/Cloud Computing/Project/src/new5k.txt"));
		
		//Generating a random coloring of vertices
		int N = 10;	//Number of colors for coloring the graph.
		for(int i=0; i<5000; i++)	//TODO: Change this manual encoding of 5000
			colorMap.put(i, randInt(0, N, rand));
		
	 	Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		Job job = new Job(conf, "Sampling by random coloring");
		job.setJarByClass(ColorCounting.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(
				"/home/kiriti/acads/Cloud Computing/Project/src/new5k.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/home/kiriti/acads/Cloud Computing/Project/src/output"));
		if (job.waitForCompletion(true)) {
			System.out.println("Hurray!!");
		} else {
			System.out.println("Error in job ");
			System.exit(0);
		}

	}
}
