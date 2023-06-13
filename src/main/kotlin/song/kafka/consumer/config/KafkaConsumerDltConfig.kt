package song.kafka.consumer.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import song.kafka.consumer.consumer.model.Message
import java.io.Serializable

@Configuration
class KafkaConsumerDltConfig {

    @Value(value = "\${spring.kafka.bootstrap-servers}")
    lateinit var server: String

    @Bean("dltKafkaListenerContainerFactory")
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Message> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, Message>()

        factory.setCommonErrorHandler(retryHandler())
        factory.consumerFactory = dltConsumerFactory()

        return factory
    }

    @Bean
    fun kafkaProducerTemplate(): KafkaTemplate<String, Message> =
        KafkaTemplate(DefaultKafkaProducerFactory(producerConfigs()))

    @Bean
    fun producerConfigs(): Map<String, Serializable> =
        mapOf(
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java
        )

    @Bean
    fun retryHandler(): DefaultErrorHandler {
        val recoverer = DeadLetterPublishingRecoverer(kafkaProducerTemplate())
        val backOff = ExponentialBackOffWithMaxRetries(6) // 에러 발생 시 재시도 횟수 since 2.7.3
        backOff.initialInterval = 1000L // 최초 재시도 주기
        backOff.multiplier = 2.0 // 제시도 곱하기 시간
        backOff.maxInterval = 10000L // 최대 재시도 주기

        return DefaultErrorHandler(recoverer, backOff)
    }

    @Bean
    fun dltConsumerFactory(): ConsumerFactory<String, Message> {
        val deserializer = JsonDeserializer(Message::class.java)
        deserializer.ignoreTypeHeaders()

        return DefaultKafkaConsumerFactory(
            dltConsumerConfigurations(),
            StringDeserializer(),
            deserializer
        )
    }

    @Bean
    fun dltConsumerConfigurations(): Map<String, Serializable> =
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to JsonDeserializer::class.java,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
        )
}
