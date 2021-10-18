package finalSolution;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SingleDeviceDataLoader implements Runnable{
    int device_id;
    DateTime start_time;
    int no_of_records;
    int operator_id;
    Map<String,Object> request;
    RestHighLevelClient restHighLevelClient;

    public SingleDeviceDataLoader(int device_id, DateTime start_time, int no_of_records, RestHighLevelClient restHighLevelClient) {
        this.device_id = device_id;
        this.start_time = start_time;
        this.no_of_records = no_of_records;
        this.operator_id = generateOperatorId();
        this.restHighLevelClient = restHighLevelClient;
    }
    public int generateOperatorId()
    {
        Random random = new Random();
        //operator_id = random.nextInt(100);
        operator_id = 1;
        return operator_id;
    }
    public void createRequest(int duration,List<Map<String,Object>> deviceList)
    {
        //return list and then update in main function
        //System.out.println("Before Request  time:"+this.start_time +" thread name:"+Thread.currentThread().getName());
        Map<String ,Object>  objectMap= new HashMap<>();
        DateTime current_time = start_time.plusSeconds(duration);
        LocalDate localDate = current_time.toLocalDate();

        objectMap.put("device_id",device_id);
        objectMap.put("ms_time",current_time);
        objectMap.put("operator_id",operator_id);

        String index_name = operator_id + "_v2_metrics_" + localDate;
        objectMap.put("index",index_name);
        System.out.println("Request :"+objectMap+" thread name:"+Thread.currentThread().getName() + " Index name:" + index_name);
        deviceList.add(objectMap);
        //bulkRequest.add(new IndexRequest("book_index","_doc").source(request, XContentType.JSON));
    }
    //put in diff class
    public void save_data(List<Map<String,Object>> deviceList) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        //RestHighLevelClient restHighLevelClient = getClient();
        for(int i=0;i<deviceList.size();i++)
        {
            System.out.println("+++ "+ deviceList.get(i));
            IndexRequest indexRequest = new IndexRequest((String) deviceList.get(i).get("index")).source(deviceList.get(i),XContentType.JSON);
            bulkRequest.add(indexRequest);
            System.out.println("end");
        }


        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);// use bulk_async
        System.out.println(bulkResponse.status());
    }
    //use autowiring
    public static RestHighLevelClient getClient()
    {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
        return client;
    }
    @Override
    public void run() {
        List<Map<String,Object>> deviceList = new ArrayList<>();
        int ms_time = 0, batch_size = 100;
        int time_interval = 10;
//mstime = start_time + duration
        for(int i=0;i<no_of_records;i++)
        {
            createRequest(ms_time,deviceList);// deviselist.add

            if(deviceList.size()== batch_size)
            {

                System.out.println("Save the data into elasticsearch and clear the list");
                try {
                    save_data(deviceList);
                    deviceList.clear();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            ms_time = ms_time + time_interval;
        }
        if(!deviceList.isEmpty())
        {
            System.out.println("Save the remaining data into elasticsearch and clear the list");
            System.out.println("List :"+deviceList+ "thread name:"+ Thread.currentThread().getName());
            try {
                 save_data(deviceList);
            } catch (IOException e) {
                e.printStackTrace();
            }
            deviceList.clear();
        }
    }
}
