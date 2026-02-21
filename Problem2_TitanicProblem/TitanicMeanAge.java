/*** Description: Map Reduce Program to find the average/mean age of male and female died in titanic disaster         ***/
/*** Input File: Plain text file in following format                                                                  ***/
/*** |PassengerID | Survived(0/1) | PClass | Name | Sex | Age | SibSp | Parch | Ticket | Fare | Cabin | Embarked|     ***/


import java.io.IOException;

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

import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;



public class TitanicMeanAge {

    public static class MeanAgeMapper extends Mapper < LongWritable, Text, Text, IntWritable > {
        private Text gender = new Text(); // to store gender
        private IntWritable age = new IntWritable(); // to store age

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString(); // row in dataset to string
            String str[] = line.split(","); // split by comma

            if (str.length > 6) { // number of column is more than 6 (if age is present?)

                gender.set(str[4]); // gender

                if ((str[1].equals("0"))) { /// person died or not

                    if (str[5].matches("\\d+")) { // regex to check number

                        int i = Integer.parseInt(str[5]); // convert to integer

                        age.set(i);
                    }
                }
            }
            context.write(gender, age);
        }
    }

    public static class MeanAgeReducer extends Reducer < Text, IntWritable, Text, IntWritable > {
        // key and list of values pair and aggregation is done based on keys
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable < IntWritable > values, Context context) throws IOException, InterruptedException {
            int sum = 0; // sum of people
            int n = 0; // number of people

            for (IntWritable val: values) {
                n += 1;
                sum += val.get();
            }
            sum = sum / n; // average
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "titanic_example");

        job.setJarByClass(TitanicMeanAge.class);
        job.setMapperClass(MeanAgeMapper.class);
        job.setCombinerClass(MeanAgeReducer.class);
        job.setReducerClass(MeanAgeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
