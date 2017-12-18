import java.io.IOException;

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

public class CoOccurrenceMatrixGenerator {
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//value = userid\t business1:rating business2:rating....
			String[] line = value.toString().trim().split("\t");
			if (line.length != 2) return;
			String[] businessRatings = line[1].split(",");
			for (String businessRating1 : businessRatings) {
				for (String businessRating2 : businessRatings) {
					context.write(new Text(businessRating1.trim().split(":")[0] + ":" + businessRating2.trim().split(":")[0]), new IntWritable(1));
				}
			}
		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable value: values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);

		job.setJarByClass(CoOccurrenceMatrixGenerator.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}
}
