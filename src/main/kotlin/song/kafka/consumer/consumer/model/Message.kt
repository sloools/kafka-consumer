package song.kafka.consumer.consumer.model

data class Message(
    val name: String?,
    val message: String?,
    var timestamp: String?
)

