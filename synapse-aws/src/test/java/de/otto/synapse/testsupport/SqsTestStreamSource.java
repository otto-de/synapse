package de.otto.synapse.testsupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static java.util.Collections.singletonMap;

/**
 * This class is a data source for supplying input to the Amazon Kinesis stream. It reads lines from the
 * input file specified in the constructor and emits them by calling String.getBytes() into the
 * stream defined in the KinesisConnectorConfiguration.
 */
public class SqsTestStreamSource {

    private static final Logger LOG = LoggerFactory.getLogger(SqsTestStreamSource.class);

    private final String channelName;
    private final SQSAsyncClient sqsAsyncClient;
    private final String inputFile;
    private final ObjectMapper objectMapper;

    public SqsTestStreamSource(SQSAsyncClient sqsAsyncClient, String channelName, String inputFile) {
        this.sqsAsyncClient = sqsAsyncClient;
        this.inputFile = inputFile;
        this.objectMapper = new ObjectMapper();
        this.channelName = channelName;
    }

    public void writeToStream() {

        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(inputFile);
        if (inputStream == null) {
            throw new IllegalStateException("Could not find input file: " + inputFile);
        }
        try {
            processInputStream(channelName, inputStream);
        } catch (IOException e) {
            LOG.error("Encountered exception while putting data in source stream.", e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    protected void processInputStream(final String channelName, final InputStream inputStream) throws IOException, ExecutionException, InterruptedException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                final String queueUrl = SqsChannelSetupUtils.getQueueUrl(sqsAsyncClient, channelName);
                sqsAsyncClient.sendMessage(SendMessageRequest.builder()
                        .queueUrl(queueUrl)
/*                        .messageGroupId(channelName)
                        .messageAttributes(singletonMap(
                                "key", MessageAttributeValue.builder()
                                        .stringValue("deleteMessageKey").build()))
*/
                        .messageBody(line)
                        .build()
                ).get();
            }
        }
    }

}
