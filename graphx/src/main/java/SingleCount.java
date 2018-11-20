
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class SingleCount {
    static String INPUT_PATH;
    static String OUTPUT_PATH;


    /**
     *
     * in-key: 序号
     * in-value： userid+"/"+邻接表
     * out-key：userid
     * out-value：空
     *
     */
    public static class SingleMapper extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Log.debug(value.toString().split("/")[0] + "");
            context.write(new Text(value.toString().split("/")[0]), new Text());

        }


    }


    /**
     *
     * in-key: userid
     * in-value：空
     * out-key：userid
     * out-value：以此id作为三角形最小定点的三角形个数
     *
     *
     *
     */
    public static class SingleReducer extends Reducer<Text, Text, Text, VIntWritable> {
        public static Map<String, ArrayList<String>> linkedMap = new HashMap<String, ArrayList<String>>();
        int index = 0;
        VIntWritable v = new VIntWritable();


        /*
        * 加载缓存（邻接矩阵）
        *
        *
        * */
        public void setup(Reducer.Context context) throws IOException {
            try {
                Configuration configuration = context.getConfiguration();
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(configuration);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    BufferedReader dataReader = new BufferedReader(new FileReader(cacheFiles[0].toUri().getPath()));
                    try {
                        String userFirst = "";
                        while ((line = dataReader.readLine()) != null) {
                            ArrayList<String> l = new ArrayList<String>();
                            userFirst = line.split("/")[0];
                            String tempStr = line.split("/")[1];
                            tempStr = tempStr.replaceAll(" ", "");
                            for (String str : tempStr.substring(1, tempStr.length() - 1).split(",")) {
                                l.add(str);
                            }
                            linkedMap.put(userFirst, l);
                        }

                    } catch (IOException e) {
                        Log.debug(e.getMessage());
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;

            /*
            *
            * 依次计算
            * 每个顶点和比它id大的顶点比较，看邻接节点中是否有较大的那个节点，再比对两个顶点邻接表的交集个数，即为三角形个数
            * */
            for (Map.Entry entry : linkedMap.entrySet()) {
                if (entry.getKey().toString().compareTo(key.toString()) > 0) {
                    if (linkedMap.get(key.toString()).contains(entry.getKey().toString())) {
                        count += intersect(linkedMap.get(entry.getKey().toString()), linkedMap.get(key.toString()));
                    }
                }
            }
            v.set(count);
            context.write(key, v);
        }

    }
/*
*
*
* 计算交集
*
* */
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


        return l.size();
    }


    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        INPUT_PATH = args[0];
        OUTPUT_PATH = args[1];
        Configuration conf = new Configuration();
        DistributedCache.addCacheFile(new URI(Util.CACHE_FILE_PATH), conf);
        Job job = new Job(conf, "SingleCount");
        job.setJarByClass(SingleCount.class);
        job.setMapperClass(SingleMapper.class);
        job.setReducerClass(SingleReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VIntWritable.class);


        /*
        *
        * 使用reducer 个数
        *
        * */
        job.setNumReduceTasks(6);
        FileSystem fileSystem = FileSystem.get(new URI(Util.SINGLECOUNT_COUNT_INPUT_PATH), conf);

        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH));
        }
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));


        job.waitForCompletion(true);

    }
}
