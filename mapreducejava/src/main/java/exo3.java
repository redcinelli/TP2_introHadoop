/**
 * Created by paulo on 12/10/16.
 */
/**
 * Created by paulo on 12/10/16.
 */
/**
 * Created by paulo on 11/10/16.
 */
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.String;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class exo3 {

    public enum TotPop { number }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String itr = value.toString();
            String[] vals = itr.split(";");
            String[] genders  = vals[1].split(",");
            for (String gender: genders) {
                word.set(gender);
                context.write(word, one);
                context.getCounter(TotPop.number).increment(1);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class Reduce extends Reducer<Text,IntWritable,Text,FloatWritable> {
        int mapperCounter;
        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            mapperCounter = (int)currentJob.getCounters().findCounter(TotPop.number).getValue();
        }
        private FloatWritable result = new FloatWritable();
        private float average ;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if(mapperCounter == 0){
                context.write(new Text("fail!"),new FloatWritable(0f));
                context.write(new Text("Counter is : "), new FloatWritable((float)mapperCounter));
                context.write(key, new FloatWritable((float)sum));
            }else{
                average = (float)sum / (float)mapperCounter;
                result.set(average);
                context.write(key,result);
            }
         }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "exo3");
        job.setJarByClass(exo3.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
