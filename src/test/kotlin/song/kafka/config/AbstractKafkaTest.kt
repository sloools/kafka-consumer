package song.kafka.config

import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import song.kafka.config.KafkaTestConfig

@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = [
        "listeners=PLAINTEXT://localhost:9092",
        "port=9092"
    ],
    topics = ["TEST.TOPIC"]
)
@DirtiesContext
@ActiveProfiles("test")
@Import(KafkaTestConfig::class)
abstract class AbstractKafkaTest