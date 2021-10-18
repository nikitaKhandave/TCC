package finalSolution;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.joda.time.DateTime;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataLoader {
    static final int MAX_T = 5;// CHK SERIAL by initializing max_t = 1
    public static RestHighLevelClient getClient()
    {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
        return client;
    }
    public static void main(String[] args) {
        System.out.println("===Hello Nikita =====");
        long start = System.currentTimeMillis();

        ExecutorService pool = Executors.newFixedThreadPool(MAX_T);
        RestHighLevelClient restHighLevelClient = getClient();
        int no_of_devices = 2;
        for (int i=1;i<=no_of_devices;i++)
        {
            DateTime current_time = DateTime.now();
            //take current time as input from user
            Runnable runnable = new SingleDeviceDataLoader(i,current_time,8,restHighLevelClient);
            pool.execute(runnable);
        }
        pool.shutdown();
        while (!pool.isTerminated()) {
        }

        long end = System.currentTimeMillis();
        long elapsedTime = end - start;
        System.out.println("Finished all threads , total time in millisecond:"+elapsedTime+ " in seconds:"+(elapsedTime*1.0/1000));
    }
}
