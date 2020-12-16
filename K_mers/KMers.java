// based in default wordcount code from hadoop
// https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

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
import org.apache.hadoop.util.GenericOptionsParser;

public class KMers {

    public static class KMerMapper
        extends Mapper<Object, Text, Text, IntWritable> {

        private final int sequenceSize = 9;

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
                       ) throws IOException, InterruptedException {
            String value_sequence = value.toString();
            for (int i = 0; i < value_sequence.length() - sequenceSize + 1; i++) {
                word.set(value_sequence.substring(i, i + sequenceSize));
                context.write(word, one);

                /*
                *  for all values of K
                *  for (int j = i+1; j <= value_sequence.length(); j++) {
                *       word.set(value_sequence.substring(i, j));
                *       context.write(word, one);
                *  }
                */
            }
        }
    }

    public static class KMerReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
                          ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class KMerOrderMapper extends
        Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value,
                        Context context
                       ) throws java.io.IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            IntWritable countPart = new IntWritable(Integer.parseInt(data[1]));
            Text sequencePart = new Text(data[0]);
            context.write(countPart, sequencePart);
        }
    }


    public static class KMerOrderReducer extends
        Reducer<IntWritable, Text, Text, IntWritable> {
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
                          )throws java.io.IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job_kmers_find = Job.getInstance(conf, "kmers_find_step");
        job_kmers_find.setJarByClass(KMers.class);
        job_kmers_find.setMapperClass(KMerMapper.class);
        job_kmers_find.setCombinerClass(KMerReducer.class);
        job_kmers_find.setReducerClass(KMerReducer.class);
        job_kmers_find.setOutputKeyClass(Text.class);
        job_kmers_find.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job_kmers_find, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_kmers_find, new Path(args[1]));

        job_kmers_find.waitForCompletion(true);

        Configuration conf_sort = new Configuration();
        Job job_kmers_sort = Job.getInstance(conf, "kmers_sort_step");
        job_kmers_sort.setJarByClass(KMers.class);
        job_kmers_sort.setMapperClass(KMerOrderMapper.class);
        job_kmers_sort.setReducerClass(KMerOrderReducer.class);
        job_kmers_sort.setOutputKeyClass(IntWritable.class);
        job_kmers_sort.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job_kmers_sort, new Path(args[1]));
        FileOutputFormat.setOutputPath(job_kmers_sort, new Path(args[2]));

        System.exit(job_kmers_sort.waitForCompletion(true) ? 0 : 1 );
    }
}
