import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MissingDataGenerator {
	public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] line = value.toString().trim().split(",");
			// line: [user, business, rating]
			context.write(new IntWritable(Integer.parseInt(line[0])), new Text(line[1] + ":" + line[2]));
		}
	}

	public static class DataFillReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// key = userID
			// value = [businessId:rating, ...]
            double sum = 0.0;
            int count = 0;
            Map<String, Double> ratingMap = new HashMap<String, Double>();
			for (Text value : values) {
				count++;
                String[] businessRating = value.toString().split(":");
                ratingMap.put(businessRating[0], Double.parseDouble(businessRating[1]));
                sum += Double.parseDouble(businessRating[1]);
			}
            Double average = sum / count;
            for (int i = 1; i <= 17770; i++) {
                String businessId = String.valueOf(i);
                // output: userId \t businessId:rating
                if (ratingMap.containsKey(businessId)) {
                    context.write(key, new Text(businessId + ":" + String.valueOf(ratingMap.get(businessId))));
                }
                else {
                    context.write(key, new Text(businessId + ":" + String.valueOf(average)));
                }
            }
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(DataFillReducer.class);

		job.setJarByClass(MissingDataGenerator.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
