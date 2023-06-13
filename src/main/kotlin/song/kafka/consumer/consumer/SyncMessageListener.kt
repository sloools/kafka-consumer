package song.kafka.consumer.consumer

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.listener.ConsumerAwareMessageListener
import org.springframework.stereotype.Component
import song.kafka.consumer.consumer.model.Message

@Component
class SyncMessageListener : ConsumerAwareMessageListener<String, Message> {
    val logger = LoggerFactory.getLogger(SyncMessageListener::class.java)

    /***
     * Message Listener를 상속받아 commit하는 방법
     */
    @KafkaListener(
        topics = ["sample_topic"],
        groupId = "my-group-2",
        containerFactory = "SycnMessageKafkaListenerContainerFactory"
    )
    override fun onMessage(data: ConsumerRecord<String, Message>, consumer: Consumer<*, *>) {
        try {
            logger.info(">>>> Message Listener message ${data.value().message}")
            consumer.commitSync()
        } catch (e: Exception) {
            logger.info("=====================error occurred======================")
            e.printStackTrace()
        } finally {
            logger.info("=====================consumer close======================")
            // close() 호출하면 This consumer has already been closed 에러 발생함.
//            consumer.close()
        }
    }
}
