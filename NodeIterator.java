import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;

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

public class NodeIterator {

	public static LinkedHashMap<Integer, Integer> degreeMap = null;

	/**
	 * @param args
	 */
	// public static class TokenMapper extends Mapper<KEYIN, VALUEIN, KEYOUT,
	// VALUEOUT>

	public static class Mapper1 extends
			Mapper<Object, Text, IntWritable, IntWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String eachLine = value.toString();
			String[] edge = eachLine.split(" ");

			// System.out.println("Hey Macha "+ eachLine);

			IntWritable u = new IntWritable(Integer.parseInt(edge[0]));

			IntWritable v = new IntWritable(Integer.parseInt(edge[1]));

			System.out.println("Hey Macha u : " + u.get() + " v : " + v.get());

			int degV = degreeMap.get(v.get());
			int degU = degreeMap.get(u.get());
			// if (deg v > deg u || deg v= deg u and v > u)
			if (degV > degU || (degV == degU && v.get() > u.get())) {
				context.write(u, v);
			}
			// else do nothing
		}
	}

	/**
	 ** The reducer class of WordCount
	 * */
	public static class Reducer1 extends
			Reducer<IntWritable, IntWritable, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<IntWritable> neighbours,
				Context context) throws IOException, InterruptedException {
			Text edge = new Text();
			ArrayList<Integer> neighbourList = new ArrayList<Integer>();
			for (IntWritable u : neighbours) {
				neighbourList.add(u.get());
			}

			for (int i = 0; i < neighbourList.size(); i++) {
				for (int j = 0; j < neighbourList.size(); j++) {

					int u = neighbourList.get(i);
					int w = neighbourList.get(j);

					int degW = degreeMap.get(w);
					int degU = degreeMap.get(u);

					// if (deg v > deg u || deg v= deg u and v > u)
					if ((degW > degU || (degW == degU && w > u)) && j != i) {
						edge.set(String.valueOf(u) + " " + String.valueOf(w));
						context.write(key, edge);
					}
				}
			}

		}
	}

	public static class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			IntWritable v = null;
			String t = value.toString();
			String[] vertices = t.split("\\s+");
			int vv = Integer.parseInt(vertices[0]);
			if (vv >= 0) {
				v = new IntWritable(vv);
			} else {
				v = new IntWritable(-1);
			}
			String ed = String.valueOf(vertices[1]) + " "
					+ String.valueOf(vertices[2]);
			context.write(new Text(ed), v);
		}
	}

	public static class Reducer2 extends
			Reducer<Text, IntWritable, IntWritable, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> vertices,
				Context context) throws IOException, InterruptedException {

			boolean flag = false;
			for (IntWritable v : vertices) {
				if (v.get() == -1) {
					flag = true;
					break;
				}
			}
			if (flag) {
				for (IntWritable v : vertices) {
					context.write(v, new IntWritable(1));
				}
			}

		}
	}

	public static class Mapper3 extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String vertex_triangle = value.toString();
			String[] svt = vertex_triangle.split("\\s+");
			context.write(new Text(svt[0]),
					new IntWritable(Integer.parseInt(svt[1])));
		}
	}

	public static class Reducer3 extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> count,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable c : count) {
				sum += c.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		String path = "/home/raghav/Documents/projects/Triangle/src/";
		
		degreeMap = new LinkedHashMap<Integer, Integer>();

		for (int i = 0; i <= 5000; i++) {
			degreeMap.put(i, 0);
		}
		BufferedReader br = new BufferedReader(new FileReader(
				path+"pd.txt"));
		try {
			System.out.println("entered!");
			String line = br.readLine();

			while (line != null) {

				String[] degreeVertex = line.split(" ");
				degreeMap.put(Integer.parseInt(degreeVertex[0]),
						Integer.parseInt(degreeVertex[1]));
				line = br.readLine();
			}
		} catch (Exception e) {
			System.out.println(e.toString());
		}

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Job job = new Job(conf, "NodeIterator Map reduce implementation");
		job.setJarByClass(NodeIterator.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(
				path+"proper_dataset.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				path+"output1"));

		if (job.waitForCompletion(true)) {
			System.out.println("Hurray!!");
		} else {
			System.out.println("Error in job ");
			System.exit(0);
		}

		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "Counting the triangles");
		job2.setJarByClass(NodeIterator.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);

		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat
				.addInputPaths(
						job2,
						path+"output1/part-r-00000,"+path+"apd.txt");
		/*
		 * FileInputFormat .addInputPath( job2, new Path(
		 * "/home/raghav/Documents/projects/Triangle/src/output/part-r-00000"));
		 */
		FileOutputFormat.setOutputPath(job2, new Path(
				path+"finaloutput1"));
		if (job2.waitForCompletion(true)) {
			System.out.println("Hurray2!!");
		} else {
			System.out.println("Error in job2");
			System.exit(0);
		}

		Configuration conf3 = new Configuration();
		Job job3 = new Job(conf3, "Counting the triangles");
		job3.setJarByClass(NodeIterator.class);
		job3.setMapperClass(Mapper3.class);
		job3.setReducerClass(Reducer3.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(IntWritable.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		FileInputFormat
				.addInputPath(
						job3,
						new Path(
								path+"finaloutput1/part-r-00000"));
		FileOutputFormat
				.setOutputPath(
						job3,
						new Path(
								path+"clustering_coeff1"));
		if (job3.waitForCompletion(true)) {
			System.out.println("Hurray3!!");
		} else {
			System.out.println("Error in job3");
			System.exit(0);
		}
	}
}
