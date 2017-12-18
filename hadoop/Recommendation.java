import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Recommendation {
	public static class ResultMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// input: <user:business \t rating>
			String[] line = value.toString().trim().split("\t");
            String[] userbusiness = line[0].split(":");
			context.write(new Text(userbusiness[0]), new Text(userbusiness[1] + ":" + line[1]));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// input: user,business,rating
			String[] line = value.toString().split(",");
			context.write(new Text(line[0]), new Text(line[1]));
		}
	}

	public static class TopReducer extends Reducer<Text, Text, Text, Text> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// input: <user, [business:rating, business, ...]>
			// value contains ':' from result matrix, else from raw InputStreamReader
            Set<String> visited = new HashSet<String>();
            final Map<String, Double> ratingMap = new HashMap<String, Double>();
            for (Text value : values) {
                if (value.toString().contains(":")) {
                    String[] businessRating = value.toString().split(":");
                    ratingMap.put(businessRating[0], Double.valueOf(businessRating[1]));
                }
                else {
                    visited.add(value.toString());
                }
            }
            Queue<String> top = new PriorityQueue<String>(10, new Comparator<String>() {
                @Override
                public int compare(String s1, String s2) {
                    return Double.compare(ratingMap.get(s1), ratingMap.get(s2));
                }
            });
            for (Map.Entry<String, Double> businessRating : ratingMap.entrySet()) {
                if (!visited.contains(businessRating.getKey())) {
                    top.offer(businessRating.getKey());
                }
                if (top.size() > 10) top.poll();
            }
            StringBuilder result = new StringBuilder();
            while (!top.isEmpty()) {
                String business = top.poll();
                result.insert(0, "," + business + ":" + ratingMap.get(business).toString());
            }
            context.write(key, new Text(result.toString().replaceFirst(",", "")));
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, ResultMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(ResultMapper.class);
		job.setMapperClass(RatingMapper.class);
		job.setReducerClass(TopReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ResultMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}
}
