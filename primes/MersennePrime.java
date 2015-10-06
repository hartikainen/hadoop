import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MersennePrime extends Configured implements Tool {

    public static class MersenneMapper
        extends Mapper<LongWritable, Text, NullWritable, LongWritable> {

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            long prime;
            double result;
            String line, token;
            StringTokenizer tokenizer;

            line = value.toString().trim();
            tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens) {
                try {
                    token = tokenizer.nextToken();
                    prime = Long.parseLong(token);

                    if ((prime & (prime - 1)) == 0) {
                        context.write(NullWritable.get(),
                                      new LongWritable(prime));
                    }
                } catch (Exception e) {} // ignore exceptions for now
            }
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException();
        }

        Job job = new Job(getConf());
        jpb.setJarByClass(MersennePrime.class);
        job.setJobName("MersennePrime");

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(MersenneMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileInputFormat.setInputPaths(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new MarsennePrime(), args);
        System.exit(ret);
    }
}
