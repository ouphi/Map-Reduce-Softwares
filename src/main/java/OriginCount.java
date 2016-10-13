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

public class OriginCount {

    public static class OriginMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        //output value
        private final static IntWritable one = new IntWritable(1);
        //output key origin
        private Text origin = new Text();

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
            String[] parts = value.toString().split(";");
            /* get the content of the third column (parts[2]) and split into words separated by ,
            * for example if part[2] = "french,spanish"
            * we will have :
            * itr.tokenNumber(0) = "french"
            * itr.tokenNumber(1) = "spanish"*/
            StringTokenizer itr = new StringTokenizer(parts[2],",");
            //write each word (origin) in context
            while (itr.hasMoreTokens()) {
                origin.set(itr.nextToken());
                context.write(origin, one);
            }
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //count origin by name
        Job jobCountOrigin = Job.getInstance(conf, "count origin");
        jobCountOrigin.setJarByClass(OriginCount.class);
        jobCountOrigin.setMapperClass(OriginMapper.class);
        jobCountOrigin.setCombinerClass(IntSumReducer.class);
        jobCountOrigin.setReducerClass(IntSumReducer.class);
        jobCountOrigin.setOutputKeyClass(Text.class);
        jobCountOrigin.setOutputValueClass(IntWritable.class);
        //define input and output
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileInputFormat.addInputPath(jobCountOrigin, input);
        FileOutputFormat.setOutputPath(jobCountOrigin, output);
        System.exit(jobCountOrigin.waitForCompletion(true) ? 0 : 1);
    }
}