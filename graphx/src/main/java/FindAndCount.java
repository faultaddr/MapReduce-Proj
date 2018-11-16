import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.Log;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class FindAndCount {

    static String INPUT_PATH = "/user/2018st07/output/par*";
    static String OUTPUT_PATH = "/user/2018st07/count";
    public static class FindMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString());
            while (it.hasMoreTokens()) {
                context.write(new Text(it.nextToken()),new Text(it.nextToken()));
            }
        }
    }


    public static class CountReducer extends Reducer<Text, Text, Text, VIntWritable> {

        int count = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text value:values){
                count+=Integer.parseInt(value.toString());
            }

            Log.info(key.toString() + "   count= " + count);
            context.write(key, new VIntWritable(count));

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf=new Configuration();
        Job job=new Job(conf,"FindAndCount");
        job.setJarByClass(FindAndCount.class);
        job.setMapperClass(FindMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VIntWritable.class);
        job.setNumReduceTasks(20);
        FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH));
        }
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        job.waitForCompletion(true);

    }
}