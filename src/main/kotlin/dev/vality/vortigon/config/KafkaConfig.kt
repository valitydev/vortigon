package dev.vality.vortigon.config

import com.rbkmoney.kafka.common.exception.handler.SeekToCurrentWithSleepBatchErrorHandler
import dev.vality.machinegun.eventsink.MachineEvent
import dev.vality.mg.event.sink.service.ConsumerGroupIdService
import dev.vality.vortigon.serializer.MachineEventDeserializer
import lombok.RequiredArgsConstructor
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties

@Configuration
@RequiredArgsConstructor
class KafkaConfig(
    private val kafkaProperties: KafkaProperties,
    private val consumerGroupIdService: ConsumerGroupIdService,
    @Value("\${kafka.consumer.concurrency}")
    private val concurrencyListenerCount: Int,
    @Value("\${kafka.max.poll.records}")
    private val maxPollRecords: String
) {

    @Bean
    fun partyListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, MachineEvent> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, MachineEvent>()
        val consumerGroup: String = consumerGroupIdService.generateGroupId(PARTY_CONSUMER_GROUP_NAME)
        initDefaultListenerProperties(
            factory,
            consumerGroup,
            MachineEventDeserializer(),
            maxPollRecords
        )
        return factory
    }

    private fun <T> initDefaultListenerProperties(
        factory: ConcurrentKafkaListenerContainerFactory<String, T>,
        consumerGroup: String,
        deserializer: Deserializer<T>,
        maxPollRecords: String,
    ) {
        val consumerFactory: DefaultKafkaConsumerFactory<String, T> = createKafkaConsumerFactory(
            consumerGroup, deserializer, maxPollRecords
        )
        factory.consumerFactory = consumerFactory
        factory.setConcurrency(concurrencyListenerCount)
        factory.setBatchErrorHandler(SeekToCurrentWithSleepBatchErrorHandler())
        factory.isBatchListener = true
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
    }

    private fun <T> createKafkaConsumerFactory(
        consumerGroup: String,
        deserializer: Deserializer<T>,
        maxPollRecords: String,
    ): DefaultKafkaConsumerFactory<String, T> {
        val props: MutableMap<String, Any> = createDefaultConsumerProperties(consumerGroup)
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = maxPollRecords
        return DefaultKafkaConsumerFactory(
            props,
            StringDeserializer(),
            deserializer
        )
    }

    private fun createDefaultConsumerProperties(value: String): MutableMap<String, Any> {
        val properties = kafkaProperties.buildConsumerProperties()
        properties[ConsumerConfig.GROUP_ID_CONFIG] = value
        properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = OffsetResetStrategy.EARLIEST.name.lowercase()
        properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false

        return properties
    }

    companion object {
        const val PARTY_CONSUMER_GROUP_NAME = "party"
    }
}
