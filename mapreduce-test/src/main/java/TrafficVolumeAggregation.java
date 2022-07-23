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

public class TrafficVolumeAggregation {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, VolumeWritable>{
        VolumeWritable volumeWritable;
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] components = line.split("\t");
            int up = Integer.parseInt(components[8]);
            int down = Integer.parseInt(components[9]);
            int total = up + down;
            volumeWritable = new VolumeWritable(
                    new IntWritable(up),
                    new IntWritable(down),
                    new IntWritable(total));
            context.write(new Text(components[1]), volumeWritable);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,VolumeWritable,Text,VolumeWritable> {
        //private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<VolumeWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            //int sum = 0;

            int upSum = 0;
            int downSum = 0;
            int totalSum = 0;

            for (VolumeWritable val : values) {
                upSum += val.up.get();
                downSum += val.down.get();
                totalSum += val.total.get();
            }
            VolumeWritable result = new VolumeWritable(
                    new IntWritable(upSum),
                    new IntWritable(downSum),
                    new IntWritable(totalSum)
            );
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "traffic_volume");
        job.setJarByClass(TrafficVolumeAggregation.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(1);
        job.setOutputValueClass(VolumeWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}