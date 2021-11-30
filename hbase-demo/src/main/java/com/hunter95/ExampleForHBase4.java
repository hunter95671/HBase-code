package com.hunter95;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
//import org.apache.hadoop.hbase.coprocessor.example.generated.ExampleProtos.CountRequest.request;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

public class ExampleForHBase4 {

/*    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args) throws Throwable {
        init();

        String tableName = "201906061515_student";

        Table table = connection.getTable(TableName.valueOf(tableName));
        final ExampleProtos.CountRequest request = ExampleProtos.CountRequest.getDefaultInstance();

        Map<byte[], Long> results = table.coprocessorService(ExampleProtos.RowCountService.class,
                null, null,
                new Batch.Call<ExampleProtos.RowCountService, Long>() {
                    public Long call(ExampleProtos.RowCountService counter) throws IOException {
                        ServerRpcController controller = new ServerRpcController();
                        BlockingRpcCallback<ExampleProtos.CountResponse> rpcCallback =
                                new BlockingRpcCallback<ExampleProtos.CountResponse>();

                        counter.getRowCount(controller, request, rpcCallback);
                        ExampleProtos.CountResponse response = rpcCallback.get();
                        if (controller.failedOnException()) {
                            throw controller.getFailedOn();
                        }
                        return (response != null && response.hasCount()) ? response.getCount() : 0;
                    }
                });

        int sum = 0;
        int count = 0;

        for (Long l : results.values()) {
            sum += l;
            count++;
        }
        System.out.println("Total Row Counts = " + sum);
        System.out.println("Region Counts = " + count);
    }

    public static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }*/
}
