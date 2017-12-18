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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value: businessA:businessB \t relation
            // line: [business1:business2, rating]
            String[] line = value.toString().trim().split("\t");
            if(line.length != 2) return;
            String[] businesss = line[0].split(":");
            context.write(new Text(businesss[0]), new Text(businesss[1] + "=" + line[1]));

        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // input: <rowId, [colId=value,...]>
            // get the sum of each row as the denominator, normalize the row sum to 1
            int sum = 0;
            Map<String, Integer> colAndValueMap = new HashMap<String, Integer>();
            for (Text value: values) {
                //businessB=relation
                String[] colAndValue = value.toString().split("=");
                sum += Integer.parseInt(colAndValue[1]);
                colAndValueMap.put(colAndValue[0], Integer.parseInt(colAndValue[1]));
            }
            // transpose the co-occurence matrix
            for (Map.Entry<String, Integer> colAndValue: colAndValueMap.entrySet()) {
                context.write(new Text(colAndValue.getKey()), new Text(key.toString() + "=" + (double)colAndValue.getValue()/sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
