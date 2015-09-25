import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Orphan Pages");

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(OrphanPageReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(OrphanPages.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        String delimiters;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            this.delimiters = getRegexDelimiters(" \t,;:.?!-:@[](){}_*/");
        }

        private String getRegexDelimiters(String value){
            StringBuilder regexp = new StringBuilder("");
            regexp.append("[");

            for (int i = 0; i < value.length(); i++) {
                regexp.append(Pattern.quote(
                        Character.toString(value.charAt(i))
                ));
            }
            regexp.append("]");

            return regexp.toString();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase().trim();
            String[] result = line.split(this.delimiters);

            for (int i = 0; i < result.length; i++) {
                if (result[i] == null || result[i].length() == 0)
                    continue;

                if (i == 0)
                    context.write(new IntWritable(Integer.parseInt(result[i])), new IntWritable(0));
                else
                    context.write(new IntWritable(Integer.parseInt(result[i])), new IntWritable(1));
            }
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;

            for (IntWritable val : values)
                total += val.get();

            if (total == 0)
                context.write(key, NullWritable.get());
        }
    }
}