package song.kafka.consumer.config

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.kafka.support.serializer.JsonDeserializer
import song.kafka.consumer.consumer.model.Message
import java.io.Serializable

@Configuration
class KafkaConsumerSyncConfig {
    @Value(value = "\${spring.kafka.bootstrap-servers}")
    lateinit var server: String
    @Bean(name = ["SycnMessageKafkaListenerContainerFactory"])
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Message>? { // 메소드 이름 중요.
        val factory = ConcurrentKafkaListenerContainerFactory<String, Message>()
        factory.containerProperties.ackMode = ContainerProperties.AckMode.RECORD // Ack Mode 설정
        factory.consumerFactory = syncConsumerFactory()
        return factory
    }

    @Bean
    fun syncConsumerFactory(): ConsumerFactory<String, Message> {
        val deserializer = JsonDeserializer(Message::class.java)
        deserializer.ignoreTypeHeaders()

        return DefaultKafkaConsumerFactory(
            sycnConsumerConfigurations(),
            StringDeserializer(),
            deserializer
        )
    }

    @Bean
    fun sycnConsumerConfigurations(): Map<String, Serializable> =
        mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to server,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ErrorHandlingDeserializer::class.java,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to JsonDeserializer::class.java,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG to "consumer-1",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false"
        )
}