package song.kafka.consumer.consumer

import org.apache.kafka.clients.consumer.Consumer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.utils.KafkaTestUtils
import song.kafka.config.AbstractKafkaTest
import song.kafka.config.TEST_TOPIC
import song.kafka.consumer.consumer.model.Message
import java.time.LocalDateTime


internal class MessageListenerTest: AbstractKafkaTest() {

    @Autowired
    private lateinit var testConsumer: Consumer<String, Message>

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, Message>

    @BeforeEach
    fun setUp() {
        val message = Message("test_name", "test_message",
            LocalDateTime.of(2023, 2, 27, 0, 0, 0).toString())
        val future = kafkaTemplate.send(TEST_TOPIC, message)
        val result = future.get()

        assertThat(result.producerRecord.value())
            .isNotNull
            .isInstanceOf(Message::class.java)
    }

    @Test
    fun test_consumer_with_embedded_kafka() {
        val response = KafkaTestUtils
            .getSingleRecord(testConsumer, TEST_TOPIC)
            .value()

        val consumed = Message("test_name", "test_message",
            LocalDateTime.of(2023, 2, 27, 0, 0, 0).toString())

        assertThat(response).isNotNull
            .isInstanceOf(Message::class.java)
            .isEqualTo(consumed)
    }
}