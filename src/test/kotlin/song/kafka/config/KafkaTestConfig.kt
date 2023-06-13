package song.kafka.config

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.kafka.test.EmbeddedKafkaBroker
import song.kafka.consumer.consumer.model.Message

@TestConfiguration
class KafkaTestConfig {

    @Bean
    fun testConsumer(
        properties: KafkaProperties,
        embeddedKafkaBroker: EmbeddedKafkaBroker
    ): Consumer<String, Message> {
        val configs = properties.buildConsumerProperties()
        configs[ConsumerConfig.GROUP_ID_CONFIG] = TEST_CONSUMER_GROUP
        val consumerFactory = DefaultKafkaConsumerFactory(
                configs,
                StringDeserializer(),
                JsonDeserializer(Message::class.java)
        )
        val consumer: Consumer<String, Message> = consumerFactory.createConsumer()
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, TEST_TOPIC)
        return consumer
    }

    @Bean
    fun producerFactory(properties: KafkaProperties) =
        DefaultKafkaProducerFactory(
            properties.buildProducerProperties(),
            StringSerializer(),
            JsonSerializer<Message>()
        )

    @Bean
    fun kafkaTemplate(producerFactory:
        DefaultKafkaProducerFactory<String, Message>) =
            KafkaTemplate(producerFactory)
}