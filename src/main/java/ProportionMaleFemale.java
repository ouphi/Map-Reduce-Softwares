/**
 * Created by lementec on 13/10/2016.
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProportionMaleFemale{
    //represent number of map output
    public  enum nLine {n}

    public static class ProportionMaleFemaleMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        //number of line
        public static long nLine = 0;
        //output value
        private final static IntWritable one = new IntWritable(1);
        //output key number of Origin in the line
        private Text gender = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            /*split the line into words separated by ;
            * for example if we have the line
            * ophelie;F;french,spanish;0
            * We will have
            * parts[0] = "ophelie"
            * parts[1] = "F"
            * parts[2] = "french,spanish"
            * part[3] = 0*/
            //get number of output map
            //nLine = context.getCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
            //nLine = nLine + 1;
            String[] parts = value.toString().split(";");
            //get gender in parts[1]
            gender = new Text(parts[1]);
            //increment the counter which represent number of line
            context.getCounter(ProportionMaleFemale.nLine.n).increment(1);
            //write in context : gender as key and 1 as value
            context.write(gender, one);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
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


    public static class PercentReducer
            extends Reducer<Text,IntWritable,Text,FloatWritable> {
        //number of map output
        int mapperCounter = 0;

        //allow to get nLine.n (number of map output)
        @Override
        public void setup(Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            mapperCounter = (int) currentJob.getCounters().findCounter(nLine.n).getValue();
        }
        private FloatWritable result = new FloatWritable();


        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            float percentage = 0;
            for (IntWritable val : values) {
                if(mapperCounter != 0)
                {
                    percentage = ((float)val.get()/(float)mapperCounter)*100;
                }else{
                    percentage = -1;
                }
            }
            result.set(percentage);
            context.write(key, result);
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //count origin by name
        Job jobPropMaleFemale = Job.getInstance(conf, "proportion gender");
        jobPropMaleFemale.setJarByClass(OriginCount.class);
        jobPropMaleFemale.setMapperClass(ProportionMaleFemaleMapper.class);
        jobPropMaleFemale.setCombinerClass(IntSumReducer.class);
        jobPropMaleFemale.setReducerClass(PercentReducer.class);
        jobPropMaleFemale.setOutputKeyClass(Text.class);
        jobPropMaleFemale.setOutputValueClass(IntWritable.class);
        //define input and output
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileInputFormat.addInputPath(jobPropMaleFemale, input);
        FileOutputFormat.setOutputPath(jobPropMaleFemale, output);
        System.exit(jobPropMaleFemale.waitForCompletion(true) ? 0 : 1);
    }
}