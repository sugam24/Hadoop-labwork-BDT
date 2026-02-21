/*** Description: Map Reduce Program to count the number of words in a given plain test file ***/
/*** Input File: Plain text file OR folder having plain text files 							 ***/


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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class WordCount {

    public static class TokenizerMapper extends Mapper < Object, Text, Text, IntWritable > {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer < Text, IntWritable, Text, IntWritable > {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable < IntWritable > values, Context context) throws IOException,
        InterruptedException {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // read configuration of the hadoop cluster from configuration XML files
        Configuration conf = new Configuration();

        // initialize job with default configuration of the hadoop cluster 
        Job job = Job.getInstance(conf, "word_count_example"); //Job job = new Job(conf, "word_count_example");

        // Drive class name
        job.setJarByClass(WordCount.class);

        // mapper class name
        job.setMapperClass(TokenizerMapper.class);

        // combiner class name
        job.setCombinerClass(IntSumReducer.class);

        // reducer class name
        job.setReducerClass(IntSumReducer.class);

        // key type from mapper
        job.setMapOutputKeyClass(Text.class);

        // value type from mapper
        job.setMapOutputValueClass(IntWritable.class);

        // output key type
        job.setOutputKeyClass(Text.class);

        // output value type
        job.setOutputValueClass(IntWritable.class);

        // input format class responsible to parse the data into key value pair
        job.setInputFormatClass(TextInputFormat.class);

        // output format class responsible to parse the data into key value pair
        job.setOutputFormatClass(TextOutputFormat.class);

        // input path
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // output path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // exiting the job only if the waiforcomplete flag value becomes false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
