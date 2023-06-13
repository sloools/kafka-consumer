package song.kafka.consumer.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.util.backoff.FixedBackOff
import song.kafka.consumer.consumer.model.Message
import java.io.Serializable

@EnableKafka
@Configuration
class KafkaConsumerConfig {

    @Value(value = "\${spring.kafka.bootstrap-servers}")
    lateinit var server: String

    @Value(value = "\${kafka.backoff.interval}")
    lateinit var interval: String

    @Value(value = "\${kafka.backoff.max-failure}")
    lateinit var maxAttempts: String

    @Bean(name = ["MessageKafkaListenerContainerFactory"])
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Message> { // 메소드 이름 중요.
        val factory = ConcurrentKafkaListenerContainerFactory<String, Message>()
        factory.containerProperties.setConsumerRebalanceListener(KafkaConsumerRebalanceHandler())
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
        factory.consumerFactory = consumerFactory()
        return factory
    }

    @Bean
    fun consumerFactory(): ConsumerFactory<String, Message> {
        val deserializer = JsonDeserializer(Message::class.java)
        deserializer.ignoreTypeHeaders()

        return DefaultKafkaConsumerFactory(
            consumerConfigurations(),
            StringDeserializer(),
            deserializer
        )
    }

    @Bean
    fun consumerConfigurations(): Map<String, Serializable> =
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
//            ConsumerConfig.GROUP_ID_CONFIG to "my-group-1",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to JsonDeserializer::class.java,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )

    @Bean
    fun errorHandler(): DefaultErrorHandler {
        val fixedBackOff = FixedBackOff(interval.toLong(), maxAttempts.toLong())
        return DefaultErrorHandler(fixedBackOff)
    }
}
