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
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;

import org.apache.commons.collections.IteratorUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.logging.Logger;

/*
 *有向图变为无向图
 *
 * */


public class UnDirectedGraph {
    static String INPUT_PATH = "hdfs://panyunyi-pc:9000/data/twitter_graph_v2.txt";
    static String OUTPUT_PATH = "hdfs://panyunyi-pc:9000/output";


    public static int intersect(List<String> arr1, List<String> arr2) {
        List<String> l = new LinkedList<String>();
        Set<String> common = new HashSet<String>();
        for (String str : arr1) {
            if (!common.contains(str)) {
                common.add(str);
            }
        }
        for (String str : arr2) {
            if (common.contains(str)) {
                l.add(str);
            }
        }

        int length = l.size();
        return length;
    }

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
                    } else {
                        context.write(id2, id1);
                        graphSet.add(tempStr2);
                    }
                }
            }

        }
    }


    public static class UserReducer extends Reducer<Text, Text, Text, Text> {
        public static Map<String, List<String>> userMap = new HashMap<String, List<String>>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String userFirst = key.toString();
            //String result;
            for (Text value : values) {

                String userSecond = value.toString();

                if (userMap.containsKey(userFirst)) {
                    if (!userMap.get(userFirst).contains(userSecond)) {
                        userMap.get(userFirst).add(userSecond);
                    }
                } else {
                    List<String> mList = new ArrayList<String>();
                    mList.add(userSecond);
                    userMap.put(userFirst, mList);
                }

            }
            //result = userMap.get(userFirst).toString();
            context.write(new Text(userFirst), new Text(""));
        }

    }

    public static class CountReducer extends Reducer<Text, Text, Text, VIntWritable> {


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            Log.info(key.toString());
//            String tempStr = "";
//            for (Text value : values) {
//                tempStr = (value.toString()).substring(1, value.toString().length() - 1);
//            }
//            String list[] = tempStr.split(",");
//            List<String> newList = Arrays.asList(list);
//            countMap.put(key.toString(), newList);
//            countMapTemp.put(key.toString(), newList);
//            countMap.putAll(UserReducer.userMap);
//            countMapTemp.putAll(UserReducer.userMap);
            Iterator iterY = UserReducer.userMap.entrySet().iterator();
            String mKey = key.toString();

            while (iterY.hasNext()) {
                Map.Entry entry = (Map.Entry) iterY.next();
                String y = (String) entry.getKey();
                if (UserReducer.userMap.get(mKey).contains(y)) {
                    List<String> x = null;
                    int order = mKey.compareTo(y);

                    if (order < 0) {

                        int temp = intersect(UserReducer.userMap.get(mKey), (List<String>) entry.getValue());
                        count += temp;

                    }
                }
            }
            //Log.info(key.toString() + "   count= " + count);
            context.write(new Text(mKey), new VIntWritable(count));

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length != 2) {
//            System.err.println("需要两个参数才能正常执行 in--out");
//            System.exit(2);
//        }

        Job job = new Job(conf);
        job.setJarByClass(UnDirectedGraph.class);
        job.setMapperClass(UserMapper.class);
        job.setCombinerClass(UserReducer.class);
        job.setReducerClass(CountReducer.class);
        job.setSortComparatorClass(Text.Comparator.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VIntWritable.class);
        FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);

        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH));
        }
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.waitForCompletion(true);
    }

}
