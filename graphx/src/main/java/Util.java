public class Util {

    /*
    *
    *
    * 本机环境
    *
    * */


    public static String UNDIRECTEDGRAPH_INPUT_PATH = "hdfs://panyunyi-pc:9000/data/gplus_combined.unique.txt";
    public static String UNDIRECTEDGRAPH_OUTPUT_PATH = "hdfs://panyunyi-pc:9000/output";

    public static String SINGLECOUNT_COUNT_INPUT_PATH="hdfs://panyunyi-pc:9000/output/part-r-00000";
    public static String SINGLECOUNT_COUNT_OUTPUT_PATH="hdfs://panyunyi-pc:9000/single_output";

    public static String FINDANDCOUNT_INPUT_PATH="hdfs://panyunyi-pc:9000/single_output/par*";
    public static String FINDANDCOUNT_OUTPUT_PATH="hdfs://panyunyi-pc:9000/count";

    public static String CACHE_FILE_PATH="hdfs://panyunyi-pc:9000/output/part-r-00000";

    /*
    *
    *
    * 集群环境
    *
    * */
//    public static String UNDIRECTEDGRAPH_INPUT_PATH = "/data/graphTriangleCount/gplus_combined.unique.txt";
//    public static String UNDIRECTEDGRAPH_OUTPUT_PATH = "/user/2018st07/output";
//
//    public static String SINGLECOUNT_COUNT_INPUT_PATH="/user/2018st07/output/part*";
//    public static String SINGLECOUNT_COUNT_OUTPUT_PATH="/user/2018st07/single_output";
//
//    public static String FINDANDCOUNT_INPUT_PATH="/user/2018st07/single_output/part*";
//    public static String FINDANDCOUNT_OUTPUT_PATH="/user/2018st07/count";
//    public static String CACHE_FILE_PATH="/user/2018st07/output/part-r-00000";


}
