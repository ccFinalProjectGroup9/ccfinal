import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: row \t col=relation
			String[] line = value.toString().trim().split("\t");
			context.write(new Text(line[0]), new Text(line[1]));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// input: user \t business:rating
			String[] userbusinessRating = value.toString().split("\t");
			String[] businessRating = userbusinessRating[1].toString().split(":");
			context.write(new Text(businessRating[0]), new Text(userbusinessRating[0] + ":" + businessRating[1]));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// input: <business, [business=relation, user:rating]>
			// value contains '=' from co-occurrence matrix, else from user rating matrix
			Map<String, Double> coMatrix = new HashMap<String, Double>();
			Map<String, Double> ratingMatrix = new HashMap<String, Double>();

			for (Text value: values) {

				if(value.toString().contains("=")) {
					String[] businessRelation = value.toString().split("=");
					coMatrix.put(businessRelation[0], Double.parseDouble(businessRelation[1]));
				}
				else {
					String[] userRating = value.toString().split(":");
					ratingMatrix.put(userRating[0], Double.parseDouble(userRating[1]));
				}
			}

			for (Map.Entry<String, Double> rowRelation : coMatrix.entrySet()) {
				for (Map.Entry<String, Double> userRating : ratingMatrix.entrySet()) {
					// <user:rowNumber, relation * rating>
					context.write(new Text(userRating.getKey() + ":" + rowRelation.getKey()), new DoubleWritable(rowRelation.getValue() * userRating.getValue()));
				}
			}

		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}
}
