import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
					if (j != i) {
						edge.set(neighbourList.get(i).toString() + " "
								+ neighbourList.get(j).toString());
						context.write(key, edge);
					}
				}
			}

		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		degreeMap = new LinkedHashMap<Integer, Integer>();

		for (int i = 0; i <= 5000; i++) {
			degreeMap.put(i, 0);
		}
		BufferedReader br = new BufferedReader(new FileReader(
				"/home/raghav/Documents/projects/Triangle/src/degree.txt"));
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
				"/home/raghav/Documents/projects/Triangle/src/new5k.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/home/raghav/Documents/projects/Triangle/src/toutput"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
