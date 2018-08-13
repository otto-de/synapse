package de.otto.synapse.endpoint.receiver.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.receiver.AbstractMessageReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static de.otto.synapse.message.Message.message;
import static org.slf4j.LoggerFactory.getLogger;

public class SqsMessageQueueReceiverEndpoint extends AbstractMessageReceiverEndpoint implements MessageQueueReceiverEndpoint {

    private static final Logger LOG = getLogger(SqsMessageQueueReceiverEndpoint.class);

    // TODO: should be configurable!

    /**
     * The visibility timeout should be high enough to process the message, otherwise messages
     * might be processed multiple times, if more than one consumer is listening to the channel.
     */
    private static final int VISIBILITY_TIMEOUT = 5;
    /**
     * Duration for long-polling calls to the SQS service
     */
    private static final int WAIT_TIME_SECONDS = 10;
    public static final MessageAttributeValue EMPTY_STRING_ATTR = MessageAttributeValue.builder().dataType("String").stringValue("").build();
    public static final String MSG_KEY_ATTR = "synapse_msg_key";

    @Nonnull
    private final SQSAsyncClient sqsAsyncClient;
    private final String queueUrl;
    private final AtomicBoolean stopSignal = new AtomicBoolean(false);

    public SqsMessageQueueReceiverEndpoint(final @Nonnull String channelName,
                                           final @Nonnull SQSAsyncClient sqsAsyncClient,
                                           final @Nonnull ObjectMapper objectMapper,
                                           final @Nullable ApplicationEventPublisher eventPublisher) {
        super(channelName, objectMapper, eventPublisher);
        this.sqsAsyncClient = sqsAsyncClient;
        try {
            this.queueUrl = sqsAsyncClient.getQueueUrl(GetQueueUrlRequest
                    .builder()
                    .queueName(channelName)
                    .build())
                    .get()
                    .queueUrl();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public CompletableFuture<Void> consume() {
        return CompletableFuture.runAsync(() -> {
            do {
                LOG.debug("Sending receiveMessage request...");
                receiveAndProcess();
            } while (!stopSignal.get());
        });
    }

    private void receiveAndProcess() {
        try {
            sqsAsyncClient.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .visibilityTimeout(VISIBILITY_TIMEOUT)
                    .waitTimeSeconds(WAIT_TIME_SECONDS)
                    .build()
            ).thenAccept(this::processResponse).get();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void processResponse(ReceiveMessageResponse response) {
        LOG.debug("Received {} messages from SQS.", response.messages().size());
        if (response.messages() != null) {
            response.messages()
                    .forEach(this::processMessage);
        }
    }

    private void processMessage(software.amazon.awssdk.services.sqs.model.Message sqsMessage) {
        LOG.debug("Processing message from channel={}: messageId={} receiptHandle={}, attributes={}, messageAttributes={}", getChannelName(), sqsMessage.messageId(), sqsMessage.receiptHandle(), sqsMessage.attributesAsStrings());
        final String key = sqsMessage.messageAttributes() != null
                ? sqsMessage.messageAttributes().getOrDefault(MSG_KEY_ATTR, EMPTY_STRING_ATTR).stringValue()
                : "";
        final Message<String> message = message(key, sqsMessage.body());

        final Message<String> interceptedMessage = intercept(message);
        if (interceptedMessage != null) {
            LOG.debug("Dispatching message {} ", interceptedMessage);
            getMessageDispatcher().accept(interceptedMessage);
        }
        deleteMessage(sqsMessage);
    }

    private void deleteMessage(software.amazon.awssdk.services.sqs.model.Message sqsMessage) {
        try {
            LOG.debug("Deleting message with receiptHandle={}", sqsMessage.receiptHandle());
            sqsAsyncClient.deleteMessage(
                    DeleteMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(sqsMessage.receiptHandle())
                            .build()
            );
        } catch (final RuntimeException e) {
            LOG.error("Error deleting message: " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Stops consumption of messages and shuts down the {@code MessageQueueReceiverEndpoint}.
     */
    @Override
    public void stop() {
        LOG.info("Channel {} received stop signal.", getChannelName());
        stopSignal.set(true);
    }

}
