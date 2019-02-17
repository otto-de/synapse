# *User Guide*

Synapse is a library used to implement eventsourcing for Spring Boot applications. 

# Message Passing

In a distributed system, service must communicate with each other. In many cases, communication is implemented using
request-response models like HTTP. Message passing is another option: tools like ActiveMQ, RabbitMQ, Kafka or Kinesis 
provide an infrastructure, to build such kind of message-passing architectures.

No matter, which solution you are using, you must decide which kind of message you want to send:
 
## Message

    `de.otto.synapse.message.Message`

A `Message` is the basic unit of communication in a message-passing system. The most simple message will only carry 
some payload like, for example, an ID, an object, a command or event. Messages have no special intent.

Having no special intent makes messages generic, but also less meaningful. That's why we can use two more concepts on
top of messages.

In Synapse, messages are represented using `de.otto.synapse.message.Message`
 
See [EIP: Message](https://www.enterpriseintegrationpatterns.com/patterns/messaging/Message.html)

## Event

    There is currently no special representation in Synapse. Use `de.otto.synapse.message.Message` instead.

An event IS-A message which informs various listeners about something which has already happened.

![EIP Event Message](https://www.enterpriseintegrationpatterns.com/img/EventMessageSolution.gif)

Events are sent by [message senders](#MessageSender) which doesn't know (and don't case) about the 
[consumers](#MessageConsumer) of the event. 

A typical example would be an online shop. Whenever an order is placed, the shop would publish an 
`OrderSubmittedEvent` to inform other systems (e.g. the logistics system) about the new order. However, the shop 
doesn’t care – or even know – about the consumers. If the logistics system isn’t interested in the events 
anymore, it just unsubscribes from the list of consumers.

Because multiple consumers might be interested in receiving events, topics or [message logs](#MessageLog) should be 
used to transmit events (one-to-many).

See [EIP: Event Message](https://www.enterpriseintegrationpatterns.com/patterns/messaging/EventMessage.html)

## Command

    There is currently no special representation in Synapse. Use `de.otto.synapse.message.Message` instead.

A command IS-A message that carries the request to execute some action. Most of the time, commands will be transferred
using point-to-point connections aka [message queues](#MessageQueue).

![EIP: Command Message](https://www.enterpriseintegrationpatterns.com/img/CommandMessageSolution.gif)

In case of an online shop, one such example could be the `BillCustomerCommand`. After an order is placed, the online
sop sends this command to the billing system to trigger the invoice.

The difference between messages, events and commands lies in their intent. While messages have no special intent at all,
events inform about something which has happened and is already completed (in the past). Commands trigger something
which should happen (in the future).
   
See [EIP: Command Message](https://www.enterpriseintegrationpatterns.com/patterns/messaging/CommandMessage.html)

## Key

    `de.otto.synapse.message.Key`

Every Synapse `Message` has a `Key` that is especially used for partitioning and compaction purposes. A key is either
simple key (`de.synapse.message.SimpleKey`) containing something like an entity-id, or a compound key 
(`de.synapse.message.CompoundKey`) consisting of a partition-key and a compaction-key. 

````java
final Key simpleKey = Key.of("urn:product:42");
final Key compoundKey = Key.of("urn:product:42", "urn:product:price:42");
````

It is recommended to not rely on message keys for purposes other than message handling. The `Key#partitionKey()` for 
example, should not be used to identify business entities. Data inside the message payload should be used instead, 
even if this involves redundant data beeing transferred in a message. 
* Different messaging systems might have restrictions regarding the length of message keys. 
* You might need to change message keys because of infrastructural changes or other changes in the message handling.
  Changes in the numbers of partitions, introduction of message compaction, etc. pp.  

## Header

    `de.otto.synapse.message.Header`

Synapse is supporting a number of default headers that are defined in `de.synapse.message.DefaultHeaderAttr`.

* `synapse_msg_id`: Unique identifier of the message.
* `synapse_msg_sender`: The name of the message-sender.
* `synapse_msg_arrival_ts`: The header attribute containing the timestamp to which the message has arrived in Kinesis. 
    Other channel implementations may use this attribute for similar timestamps, as long as the semantics of the property 
    is the met.
* `synapse_msg_sender_ts`: The timestamp to which the message was sent.
* `synapse_msg_receiver_ts`: The timestamp to which the message was received by a [](#MessageReceiverEndpoint).

Example: Reading headers
```java
void foo(final Message msg) {

    final String messageId = msg
            .getHeader()
            .getAsString(DefaultHeaderAttr.MSG_ID);
    final Instant senderTs = msg
            .getHeader()
            .getAsInstant(DefaultHeaderAttr.MSG_SENDER_TS);

    ...        
}
```

See [](#DefaultSenderHeadersInterceptor) and [](#DefaultReceiverHeadersInterceptor) for details about how and when 
these headers will be available.
 
## Payload

## MessageFormat & MessageCodec

# Channels

## Endpoints

### MessageReceiverEndpoint

### MessageQueueReceiverEndpoint

### MessageLogReceiverEndpoint

## MessageInterceptor

### DefaultSenderHeadersInterceptor

### DefaultReceiverHeadersInterceptor

## MessageSender

## MessageLog

## MessageQueue

## MessageConsumer

## StateRepository

## MessageStore

# EventSource

