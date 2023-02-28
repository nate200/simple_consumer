package omg.example.listener;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(SaveOffsetsOnRebalance.class.getName());
    private final KafkaConsumer<?,?> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> offsetsBatch;

    public SaveOffsetsOnRebalance(KafkaConsumer<?,?> consumer, Map<TopicPartition, OffsetAndMetadata> offsetsBatch) {
        this.consumer = consumer;
        this.offsetsBatch = offsetsBatch;
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // save the offsets in an external store using some custom code not described here
        log.info("commit offsets onPartitionsRevoked");
        consumer.commitSync(offsetsBatch);
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        StringBuilder sb = new StringBuilder();
        for(TopicPartition partition: partitions)
            sb.append(partition.topic() + ":" + partition.partition() + " ");

        log.info("new assigned partitions: {}", sb);
    }
}
