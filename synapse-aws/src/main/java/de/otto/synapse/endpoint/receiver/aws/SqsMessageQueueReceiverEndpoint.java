package de.otto.synapse.endpoint.receiver.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.otto.synapse.endpoint.receiver.AbstractMessageReceiverEndpoint;
import de.otto.synapse.endpoint.receiver.MessageQueueReceiverEndpoint;
import de.otto.synapse.message.Message;
import org.slf4j.Logger;
import org.springframework.context.ApplicationEventPublisher;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.sqs.SQSAsyncClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static de.otto.synapse.message.Message.message;
import static java.lang.String.format;
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
            queueUrl = sqsAsyncClient.getQueueUrl(GetQueueUrlRequest
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
    public void consume() {
        getMessageDispatcher().getAll().forEach(messageConsumer -> {
            if (!messageConsumer.keyPattern().pattern().equals(".*")) {
                // TODO: key als message attribute o.ä. senden - bug in localstack
                throw new IllegalStateException("Unable to select messages using key pattern");
            }
        });
        CompletableFuture.<Void>supplyAsync(() -> {
            do {
                LOG.debug("Sending receiveMessage request...");
                receiveAndProcess();
            } while (!stopSignal.get());
            return null;
        });
    }

    private void receiveAndProcess() {
        try {
            sqsAsyncClient.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .visibilityTimeout(VISIBILITY_TIMEOUT)
                    .waitTimeSeconds(WAIT_TIME_SECONDS)
                    .build()
            ).thenAccept(this::process).get();
        } catch (final SdkClientException e) {
            LOG.error("Caught an SdkClientException, which means " +
                    "the client encountered a serious internal problem while " +
                    "trying to communicate with Amazon SQS, such as not " +
                    "being able to access the network: " + e.getMessage(), e);
        } catch (final SQSException e) {
            LOG.error("Caught an SQSException, which means " +
                    "your request made it to Amazon SQS, but was " +
                    "rejected with an error response for some reason: " + e.getMessage(), e);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void process(ReceiveMessageResponse response) {
        if (response.messages() != null) {
            LOG.debug("Received {} messages from SQS.", response.messages().size());
            response
                    .messages()
                    .forEach(this::process);
        } else {
            LOG.warn("No messages in ReceiveMessageResponse: " + response.toString());
        }
    }

    private void process(software.amazon.awssdk.services.sqs.model.Message sqsMessage) {
        try {
            LOG.debug("Processing message from channel={}: messageId={} receiptHandle={}, attributes={}, messageAttributes={}", getChannelName(), sqsMessage.messageId(), sqsMessage.receiptHandle(), sqsMessage.attributesAsStrings());
/*            final MessageAttributeValue key = sqsMessage.messageAttributes().get("key");
            final Message<String> message = key != null
                    ? message(key.stringValue(), sqsMessage.body())
                    : message("", sqsMessage.body());
*/
            final Message<String> message = message("", sqsMessage.body());

            final Message<String> interceptedMessage = intercept(message);
            if (interceptedMessage != null) {
                LOG.debug("Dispatching message " + message);
                getMessageDispatcher().accept(message);
            }
            LOG.debug("Deleting message with receiptHandle={}", sqsMessage.receiptHandle());
        } catch (RuntimeException e) {
            // TODO: ein Error-Channel wäre hier eine feine Sache!
            // TODO: StatusDetailIndicator -> warn
            LOG.error(format("Error processing message %s: %s. Message will be ignored.", sqsMessage, e.getMessage()), e);
        }
        delete(sqsMessage);
    }

    private void delete(software.amazon.awssdk.services.sqs.model.Message sqsMessage) {
        try {
            sqsAsyncClient.deleteMessage(
                    DeleteMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .receiptHandle(sqsMessage.receiptHandle())
                            .build()
            );
        } catch (final InvalidIdFormatException e) {
            LOG.error("Error deleting message after processing it: The receipt handle isn't valid for the current version. " + e.getMessage(), e);
        } catch (final ReceiptHandleIsInvalidException e) {
            LOG.error("Error deleting message after processing it: The receipt handle provided isn't valid. " + e.getMessage(), e);
        } catch (final SdkClientException e) {
            LOG.error("Caught an SdkClientException, which means " +
                    "the client encountered a serious internal problem while " +
                    "trying to communicate with Amazon SQS, such as not " +
                    "being able to access the network: " + e.getMessage(), e);
        } catch (final SQSException e) {
            LOG.error("Caught an SQSException, which means " +
                    "your request made it to Amazon SQS, but was " +
                    "rejected with an error response for some reason: " + e.getMessage(), e);
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
