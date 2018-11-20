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

    static String INPUT_PATH;
    static String OUTPUT_PATH;

    /**
     *
     * in-key: 序号
     * in-value：id+“ ”+“数目“
     * out-key：id
     * out-value：数目
     *
     */
    public static class FindMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString());
            while (it.hasMoreTokens()) {
                context.write(new Text(it.nextToken()), new Text(it.nextToken()));
            }
        }
    }


    /**
     *
     * in-key：id
     * in-value：数目
     * out-key：id
     * out-value：累加至此的总和
     *
     *
     */
    public static class CountReducer extends Reducer<Text, Text, Text, VIntWritable> {

        int count = 0;
        VIntWritable v = new VIntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value : values) {
                count += Integer.parseInt(value.toString());
            }

            Log.info(key.toString() + "   count= " + count);
            v.set(count);
            context.write(key, v);

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        INPUT_PATH = Util.FINDANDCOUNT_INPUT_PATH;
        OUTPUT_PATH = Util.FINDANDCOUNT_OUTPUT_PATH;
        Configuration conf = new Configuration();
        Job job = new Job(conf, "FindAndCount");
        job.setJarByClass(FindAndCount.class);
        job.setMapperClass(FindMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VIntWritable.class);
        job.setNumReduceTasks(1);
        FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH));
        }
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        job.waitForCompletion(true);

    }
}