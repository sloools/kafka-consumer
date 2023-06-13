package song.kafka.consumer.consumer

import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import song.kafka.consumer.consumer.model.Message

@Component
class DltListener {
    val logger = LoggerFactory.getLogger(DltListener::class.java)
    @KafkaListener(
        topics = ["dlt-test-topic"],
        groupId = "my-group-1",
        containerFactory = "dltKafkaListenerContainerFactory"
    )
    fun listener(message: Message) {
        logger.info(">>>> Dlt KafkaListener message ${message.message}")
        throw IllegalArgumentException()
    }
}