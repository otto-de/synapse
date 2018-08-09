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
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static de.otto.synapse.channel.ChannelPosition.channelPosition;
import static de.otto.synapse.endpoint.sender.aws.SqsMessageSender.MSG_KEY_ATTR;
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

    public SqsTestStreamSource(SQSAsyncClient sqsAsyncClient, String channelName, String inputFile) {
        this.sqsAsyncClient = sqsAsyncClient;
        this.inputFile = inputFile;
        this.channelName = channelName;
    }

    public void writeToStream() {

        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(inputFile);
        if (inputStream == null) {
            throw new IllegalStateException("Could not find input file: " + inputFile);
        }
        try {
            processInputStream(channelName, inputStream);
        } catch (Exception e) {
            LOG.error("Encountered exception while putting data in source stream.", e);
        }
    }

    protected void processInputStream(final String channelName, final InputStream inputStream) throws IOException, ExecutionException, InterruptedException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                final String queueUrl = SqsChannelSetupUtils.getQueueUrl(sqsAsyncClient, channelName);
                final SendMessageResponse response = sqsAsyncClient.sendMessage(SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageAttributes(
                                singletonMap(MSG_KEY_ATTR, MessageAttributeValue.builder().dataType("String").stringValue("some-key").build()))
                        .messageBody(line)
                        .build()
                ).exceptionally(e -> {
                    LOG.error(e.getMessage(), e);
                    return null;
                        }
                ).get();
                LOG.debug("Received SendMessageResponse={}", response);
            }
        }
    }

}
