package common;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @Author: Shi Yu
 * @Date: 2019/9/2 11:01
 * @Version 1.0
 */
public class ESUtils {

    /**
     * 获取监听
     * @return
     */
    public static BulkProcessor.Listener getBulkListener(){
        BulkProcessor.Listener listener =  new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

            }
        };
        return listener;
    }


    /**
     * 获取处理器
     * @param client
     * @param listener
     * @param bulkcnumber
     * @return
     * @throws InterruptedException
     */
    public static BulkProcessor  getBulkprocessor(RestHighLevelClient client, BulkProcessor.Listener listener,int bulkcnumber) throws InterruptedException {
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) ->
                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener).build();
        BulkProcessor.Builder builder = BulkProcessor.builder(
                (request, bulkListener) ->
                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener);
        builder.setBulkActions(bulkcnumber);
        builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
        builder.setConcurrentRequests(0);
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
        builder.setBackoffPolicy(BackoffPolicy
                .constantBackoff(TimeValue.timeValueSeconds(1L), 3));
        return bulkProcessor;
    }

    public static String getMonthFirstDay(){
        Calendar cale = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        cale.add(Calendar.MONTH, 0);
        cale.set(Calendar.DAY_OF_MONTH, 1);
        return format.format(cale.getTime());
    }

    public static String getMonthLastDay(){
        Calendar cale = Calendar.getInstance();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        cale.add(Calendar.MONTH, 1);
        cale.set(Calendar.DAY_OF_MONTH, 0);
        return format.format(cale.getTime());
    }

    public static void main(String[] args) {
        System.out.println("first:" + ESUtils.getMonthFirstDay());
        System.out.println("first:" + ESUtils.getMonthLastDay());
    }
}
