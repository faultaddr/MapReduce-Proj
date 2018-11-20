
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


/*
 *有向图变为无向图
 *
 * */


public class UnDirectedGraph {
    static String INPUT_PATH;
    static String OUTPUT_PATH;


    public static class UserMapper extends Mapper<Object, Text, Text, Text> {


        private Text id1 = new Text();
        private Text id2 = new Text();
        private Set<String> graphSet = new HashSet<String>();

        /*
         *  a-b
         *  如果a>b  a放到后面
         *  如果a<b  a放到前面
         *
         *
         * */
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                id1 = new Text(itr.nextToken());
                id2 = new Text(itr.nextToken());
                String tempStr1 = id1 + " " + id2;
                String tempStr2 = id2 + " " + id1;
                if (!(graphSet.contains(tempStr1) || graphSet.contains(tempStr2))) {
                    if (id1.compareTo(id2) < 0) {
                        context.write(id1, id2);
                        graphSet.add(tempStr1);
                    } else if(id1.compareTo(id2)>0){
                        context.write(id2, id1);
                        graphSet.add(tempStr2);
                    }
                }
            }
        }
    }


    public static class UserReducer extends Reducer<Text, Text, Text, NullWritable> {
        public static ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String userFirst = key.toString();
            ArrayList<String> mList = new ArrayList<String>();
            for (Text value : values) {

                String userSecond = value.toString();
                mList.add(userSecond);
            }

            context.write(new Text(userFirst + "/" + mList.toString()), NullWritable.get());
        }

    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        INPUT_PATH = args[0];
        OUTPUT_PATH = args[1];

        Job job = new Job(conf);
        job.setJarByClass(UnDirectedGraph.class);
        job.setMapperClass(UserMapper.class);
        job.setReducerClass(UserReducer.class);
        job.setSortComparatorClass(Text.Comparator.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);

        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH));
        }
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        job.waitForCompletion(true);
    }

}
