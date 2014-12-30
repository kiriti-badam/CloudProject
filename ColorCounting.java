import java.awt.image.ColorModel;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Random;
import java.util.List;

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

		String root_path = "/home/kiriti/acads/Cloud Computing/CloudProject/";
	    Random rand = new Random();	//For generating random colors 
	    //HashMap<Integer, Integer> colorMap = new HashMap<Integer, Integer>();
	    colorMap = new HashMap<Integer, Integer>();
		
		//Generating a random coloring of vertices
		int N = 7;	//Number of colors for coloring the graph.
		for(int i=0; i<=5000; i++)	//TODO: Change this manual encoding of 5000
			colorMap.put(i, randInt(0, N-1, rand));
		
	 	Configuration conf = new Configuration();

		Job job = new Job(conf, "Sampling by random coloring");
		job.setJarByClass(ColorCounting.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(
				root_path+"proper_dataset.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				root_path+"output"));
		if (job.waitForCompletion(true)) {
			System.out.println("Hurray!!");
		} else {
			System.out.println("Error in job ");
			System.exit(0);
		}
		
		//Write the nodeIterator part to count the total number of Triangles.

		LinkedHashMap<Integer, Set<Integer>> adjmap = new LinkedHashMap<Integer, Set<Integer>>();
		BufferedReader br = new BufferedReader(new FileReader(root_path+"output/part-r-00000"));

		//Construct the adjacency list from the output of the mapreduce
		try {
			System.out.println("entered!");
			String line = br.readLine();

			while (line != null) {

				String[] vertices = line.split("\\s+");
				Integer color = Integer.parseInt(vertices[0]);
				Integer v1 = Integer.parseInt(vertices[1]);
				Integer v2 = Integer.parseInt(vertices[2]);

				if(adjmap.containsKey(v1))
				{
					Set<Integer> s = adjmap.get(v1);
					s.add(v2);
					adjmap.put(v1, s);
				}
				else
				{
					Set<Integer> s = new HashSet<Integer>();
					s.add(v2);
					adjmap.put(v1,s);
				}

				if(adjmap.containsKey(v2))
				{
					Set<Integer> s = adjmap.get(v2);
					s.add(v1);
					adjmap.put(v2, s);
				}
				else
				{
					Set<Integer> s = new HashSet<Integer>();
					s.add(v1);
					adjmap.put(v2,s);
				}
				line = br.readLine();
			}
			System.out.println(adjmap.get(0).toString());
			System.out.println(adjmap.get(0).toString());
			
			//Node iterator basic part for counting the triangles.
			double T = 0;
			Iterator<Integer> it = adjmap.keySet().iterator();
			while(it.hasNext())
			{
				Integer v = it.next();
				for(Integer u : adjmap.get(v))
				{
					for(Integer w : adjmap.get(v))
					{
						if(u!=w && adjmap.get(u).contains(w))
							T += 0.5;
					}
				}
			}
			System.out.format("Total number of triangles are %f",T/3);
			System.out.format("The value of total triangles is %f", (T/3)*N*N);

		} catch (Exception e) {
			System.out.println(e.toString());
		}
	}
}
