package song.kafka.consumer.config

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

class KafkaConsumerRebalanceHandler: ConsumerRebalanceListener {
    private val logger = LoggerFactory.getLogger(KafkaConsumerRebalanceHandler::class.java)

    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
        logger.error("파티션 재할당 됨")
    }

    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {
        logger.error("파티션 제거 됨")
    }
}