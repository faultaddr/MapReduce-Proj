import java.io.IOException;
import java.net.URISyntaxException;

public class Driver {

    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {

        UnDirectedGraph.main(new String[]{Util.UNDIRECTEDGRAPH_INPUT_PATH,Util.UNDIRECTEDGRAPH_OUTPUT_PATH});
        System.out.println("UnDirectGraph complete");

        SingleCount.main(new String[]{Util.SINGLECOUNT_COUNT_INPUT_PATH,Util.SINGLECOUNT_COUNT_OUTPUT_PATH});
        System.out.println("SingleCount complete");


        FindAndCount.main(new String[]{Util.FINDANDCOUNT_INPUT_PATH,Util.FINDANDCOUNT_OUTPUT_PATH});
        System.out.println("totally Complete");
    }
}
