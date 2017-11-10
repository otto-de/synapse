package de.otto.edison.eventsourcing.example.consumer;

import com.google.common.collect.ImmutableList;
import de.otto.edison.eventsourcing.example.consumer.configuration.MyServiceProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Component
@EnableConfigurationProperties(MyServiceProperties.class)
@EnableAsync
public class SampleProducer {

    @Autowired
    private KinesisClient kinesisClient;
    @Autowired
    private MyServiceProperties properties;


    @PostConstruct
    public void produceSampleData() {
        produceBananaSampleData();
        produceProductsSampleData();
    }

    protected void produceBananaSampleData() {
        PutRecordsRequest putRecordBatchRequest = PutRecordsRequest
                .builder()
                .streamName(properties.getBananaStreamName())
                .records(ImmutableList.of(
                        createBananaRequest("1"),
                        createBananaRequest("2"),
                        createBananaRequest("3"),
                        createBananaRequest("4"),
                        createBananaRequest("5"),
                        createBananaRequest("6")
                ))
                .build();

        kinesisClient.putRecords(putRecordBatchRequest);
    }

    protected void produceProductsSampleData() {
        PutRecordsRequest putRecordBatchRequest = PutRecordsRequest
                .builder()
                .streamName(properties.getProductStreamName())
                .records(ImmutableList.of(
                        createProductRequest("1"),
                        createProductRequest("2"),
                        createProductRequest("3"),
                        createProductRequest("4"),
                        createProductRequest("5"),
                        createProductRequest("6")
                ))
                .build();

        kinesisClient.putRecords(putRecordBatchRequest);
    }

    private PutRecordsRequestEntry createBananaRequest(String id) {
        String bananaJson = "{\"id\":\"" + id + "\", \"color\":\"red\"}";
        return PutRecordsRequestEntry
                .builder()
                .partitionKey(id)
                .data(ByteBuffer.wrap(bananaJson.getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    private PutRecordsRequestEntry createProductRequest(String id) {
        String productJson = "{\"id\":\"" + id + "\", \"price\":123}";
        return PutRecordsRequestEntry
                .builder()
                .partitionKey(id)
                .data(ByteBuffer.wrap(productJson.getBytes(StandardCharsets.UTF_8)))
                .build();
    }


}
