import java.io.IOException;
import java.net.URISyntaxException;

public class Driver {

    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {

        UnDirectedGraph.main(new String[]{});
        System.out.println("UnDirectGraph complete");

        FindAndCount.main(new String[]{});
        System.out.println("Complete");
    }
}
