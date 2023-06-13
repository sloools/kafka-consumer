package song.kafka.consumer.consumer

import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaHandler
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import song.kafka.consumer.consumer.model.Message

@Component
@KafkaListener(
    topics = ["sample_topic"],
    groupId = "my-group-1",
    containerFactory = "MessageKafkaListenerContainerFactory"
)
class MessageListener(
//    private val template: SimpMessagingTemplate
) {
    val logger = LoggerFactory.getLogger(MessageListener::class.java)

    @KafkaHandler
    fun messageListener(
        message: Message,
        ack: Acknowledgment
    ) {
        logger.info(">>>> KafkaListener message ${message.message}")
        ack.acknowledge() // commit - factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL 설정 필수
    }
}
