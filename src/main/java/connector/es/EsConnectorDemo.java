package connector.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.http.HttpHost;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentFactory;

public class EsConnectorDemo {
    public static void main(String[] args) throws Exception {
        // 读kafka消息源
        StreamExecutionEnvironment envStream = StreamExecutionEnvironment.createLocalEnvironment();
        envStream.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        Properties propsConsumer = new Properties();
        propsConsumer.setProperty("bootstrap.servers", "localhost:9092");
        propsConsumer.setProperty("group.id", "flink-test");
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), propsConsumer);
        consumer.setStartFromLatest();
        DataStream<String> stream = envStream.addSource(consumer).setParallelism(2);
        stream.print();

        // 数据处理
        final AtomicInteger atomicInteger = new AtomicInteger(1);
        DataStream<Tuple2<String, Integer>> filterStream = stream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                try {
                    System.out.println("kafka:" + value);
                    return Tuple2.<String, Integer>of(value, atomicInteger.incrementAndGet());

//                    WkNotification transferLineBO = JSON.parseObject(value, WkNotification.class);
//                    String type = transferLineBO.getType();
//                    if (type.equals("jlh_crawling_result")) {
//                        Map<String, String> map = transferLineBO.getContent();
//                        Integer status = Integer.parseInt(map.get("result"));
//                        String id = map.get("id");
//                        return Tuple2.<String, Integer>of(id, status);
//                    }
                } catch (Exception e) {
                }
                return Tuple2.of(null, null);
            }
        }).filter(s -> s.f0 != null);

        filterStream.print();

        //初始化es client链接
        HttpHost httpHost = new HttpHost("127.0.0.1", 9200, "http");
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(httpHost);
        ElasticsearchSink.Builder<Tuple2<String, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple2<String, Integer>>(httpHosts,
                new ElasticsearchSinkFunction<Tuple2<String, Integer>>() {

                    public UpdateRequest updateIndexRequest(Tuple2<String, Integer> element) throws IOException {
                        String id = element.f0;
                        Integer status = element.f1;
                        UpdateRequest updateRequest = new UpdateRequest();
                        //设置表的index和type,必须设置id才能update
                        updateRequest.index("test_index")
                                .type("_doc")
                                .id(id)
                                .docAsUpsert(true)
                                .doc(XContentFactory.jsonBuilder()
                                        .startObject()
                                        .field("status", status)
                                        .endObject());
                        return updateRequest;
                    }

                    @Override
                    public void process(Tuple2<String, Integer> element, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        try {
                            requestIndexer.add(updateIndexRequest(element));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
        );

        //必须设置flush参数
        esSinkBuilder.setBulkFlushMaxActions(1);

        filterStream.addSink(esSinkBuilder.build());

        envStream.execute("this-test");
    }
}
