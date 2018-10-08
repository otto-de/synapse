package de.otto.synapse.endpoint.receiver.aws;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class KinesisStreamInfo {

    private final String channelName;
    private final String arn;
    private final ImmutableList<KinesisShardInfo> shardInfo;

    public KinesisStreamInfo(final String channelName,
                             final String arn,
                             final ImmutableList<KinesisShardInfo> shardInfo) {
        this.channelName = channelName;
        this.arn = arn;
        this.shardInfo = shardInfo;
    }

    private KinesisStreamInfo(Builder builder) {
        channelName = builder.channelName;
        arn = builder.arn;
        shardInfo = ImmutableList.copyOf(builder.shardInfo);
    }

    public String getChannelName() {
        return channelName;
    }

    public String getArn() {
        return arn;
    }

    public ImmutableList<KinesisShardInfo> getShardInfo() {
        return shardInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KinesisStreamInfo that = (KinesisStreamInfo) o;
        return Objects.equals(channelName, that.channelName) &&
                Objects.equals(arn, that.arn) &&
                Objects.equals(shardInfo, that.shardInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(channelName, arn, shardInfo);
    }

    @Override
    public String toString() {
        return "KinesisStreamInfo{" +
                "channelName='" + channelName + '\'' +
                ", arn='" + arn + '\'' +
                ", shardInfo=" + shardInfo +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder copyOf(final KinesisStreamInfo streamInfo) {
        return new Builder(streamInfo);
    }


    public static final class Builder {
        private String channelName;
        private String arn;
        private List<KinesisShardInfo> shardInfo;

        public Builder() {
            shardInfo = new ArrayList<>();
        }

        public Builder(KinesisStreamInfo copy) {
            this.channelName = copy.getChannelName();
            this.arn = copy.getArn();
            this.shardInfo = new ArrayList<>(copy.getShardInfo());
        }

        public Builder withChannelName(String val) {
            channelName = val;
            return this;
        }

        public Builder withArn(String val) {
            arn = val;
            return this;
        }

        public Builder withShardInfo(List<KinesisShardInfo> val) {
            shardInfo = val;
            return this;
        }

        public Builder withShard(final String shardName, final boolean open) {
            shardInfo.add(new KinesisShardInfo(shardName, open));
            return this;
        }

        public KinesisStreamInfo build() {
            return new KinesisStreamInfo(this);
        }
    }
}
