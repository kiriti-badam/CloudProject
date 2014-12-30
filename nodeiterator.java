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

public class nodeiterator {

	public static void main(String[] args)
	{
		
		String root_path = "/home/kiriti/acads/Cloud Computing/CloudProject/";
		LinkedHashMap<Integer, Set<Integer>> adjmap = new LinkedHashMap<Integer, Set<Integer>>();

		//Construct the adjacency list from the output of the mapreduce
		try {
			BufferedReader br = new BufferedReader(new FileReader(root_path+"proper_dataset.txt"));
			System.out.println("entered!");
			String line = br.readLine();

			while (line != null) {

				String[] vertices = line.split("\\s+");
				Integer v1 = Integer.parseInt(vertices[0]);
				Integer v2 = Integer.parseInt(vertices[1]);

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
			System.out.println(adjmap.get(1810).toString());
			
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
		} catch (Exception e) {
			System.out.println(e.toString());
		}
	}
}
