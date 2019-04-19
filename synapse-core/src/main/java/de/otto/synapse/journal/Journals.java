package de.otto.synapse.journal;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.messagestore.MessageStore;
import de.otto.synapse.messagestore.MessageStores;
import de.otto.synapse.messagestore.OffHeapIndexingMessageStore;
import de.otto.synapse.state.StateRepository;

import static com.google.common.base.CaseFormat.LOWER_HYPHEN;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static de.otto.synapse.messagestore.Indexers.journalKeyIndexer;

public class Journals {

    private Journals() {
    }

    public static Journal noOpJournal() {
        return new Journal() {
            @Override
            public String getName() {
                return "no-op";
            }

            @Override
            public ImmutableList<String> getJournaledChannels() {
                return ImmutableList.of();
            }

            @Override
            public MessageStore getMessageStore() {
                return MessageStores.emptyMessageStore();
            }
        };
    }

    public static Journal singleChannelJournal(final StateRepository<?> stateRepository,
                                               final String channelName) {
        return singleChannelJournal(stateRepository.getName(), channelName);
    }

    public static Journal singleChannelJournal(final String name,
                                               final String channelName) {
        return new Journal() {
            final MessageStore messageStore = new OffHeapIndexingMessageStore(nameFrom(channelName, "MessageStore"), journalKeyIndexer());

            @Override
            public String getName() {
                return name;
            }

            @Override
            public ImmutableList<String> getJournaledChannels() {
                return ImmutableList.of(channelName);
            }

            @Override
            public MessageStore getMessageStore() {
                return messageStore;
            }
        };
    }

    public static Journal multiChannelJournal(final StateRepository<?> stateRepository,
                                              final String channelName,
                                              final String... moreChannelNames) {
        return multiChannelJournal(stateRepository.getName(), channelName, moreChannelNames);
    }

    public static Journal multiChannelJournal(final String name,
                                              final String channelName,
                                              final String... moreChannelNames) {
        return new Journal() {
            final MessageStore messageStore = new OffHeapIndexingMessageStore(nameFrom(channelName, "MessageStore"), journalKeyIndexer());

            @Override
            public String getName() {
                return name;
            }

            @Override
            public ImmutableList<String> getJournaledChannels() {
                if (moreChannelNames != null && moreChannelNames.length > 0) {
                    return ImmutableList.<String>builder().add(channelName).add(moreChannelNames).build();
                } else {
                    return ImmutableList.of(channelName);
                }
            }

            @Override
            public MessageStore getMessageStore() {
                return messageStore;
            }
        };
    }

    private static String nameFrom(final String channelName, final String suffix) {
        return LOWER_HYPHEN.to(UPPER_CAMEL, channelName) + suffix;
    }

}
