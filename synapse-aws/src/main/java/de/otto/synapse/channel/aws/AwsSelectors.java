package de.otto.synapse.channel.aws;

import de.otto.synapse.channel.Selectors;

public class AwsSelectors extends Selectors {

    public interface Kinesis extends MessageLog {}

    public interface Sqs extends MessageQueue {}

}
