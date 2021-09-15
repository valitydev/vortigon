package com.rbkmoney.vortigon.config

import com.rbkmoney.kafka.common.exception.handler.SeekToCurrentWithSleepBatchErrorHandler
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.mg.event.sink.service.ConsumerGroupIdService
import com.rbkmoney.vortigon.config.properties.KafkaProperties
import com.rbkmoney.vortigon.serializer.MachineEventDeserializer
import lombok.RequiredArgsConstructor
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import java.io.File

private val log = KotlinLogging.logger {}

@Configuration
@RequiredArgsConstructor
class KafkaConfig(
    private val kafkaProperties: KafkaProperties,
    private val consumerGroupIdService: ConsumerGroupIdService,
) {
    @Bean
    fun partyListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, MachineEvent> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, MachineEvent>()
        val consumerGroup: String = consumerGroupIdService.generateGroupId(PARTY_CONSUMER_GROUP_NAME)
        initDefaultListenerProperties<MachineEvent>(
            factory,
            consumerGroup,
            MachineEventDeserializer(),
            kafkaProperties.maxPollRecords
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
        factory.setConcurrency(kafkaProperties.consumer.concurrency.toInt())
        factory.setBatchErrorHandler(SeekToCurrentWithSleepBatchErrorHandler())
        factory.isBatchListener = true
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
    }

    private fun <T> createKafkaConsumerFactory(
        consumerGroup: String,
        deserializer: Deserializer<T>,
        maxPollRecords: String,
    ): DefaultKafkaConsumerFactory<String, T> {
        val props: MutableMap<String, Any> = createDefaultProperties(consumerGroup)
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = maxPollRecords
        return DefaultKafkaConsumerFactory(
            props,
            StringDeserializer(),
            deserializer
        )
    }

    private fun createDefaultProperties(value: String): MutableMap<String, Any> {
        return HashMap<String, Any>().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, value)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name.lowercase())
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaProperties.maxSessionTimeoutMs)
            put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaProperties.maxPollIntervalMs)
            putAll(createSslConfig())
        }
    }

    private fun createSslConfig(): Map<String, Any> {
        val kafkaSslProperties = kafkaProperties.ssl
        log.info("Kafka ssl isEnabled: {}", kafkaSslProperties.enabled)
        val configProps = HashMap<String, Any>()
        if (kafkaSslProperties.enabled) {
            configProps["security.protocol"] = "SSL"
            configProps["ssl.truststore.location"] = File(kafkaSslProperties.trustStoreLocation).absolutePath
            configProps["ssl.truststore.password"] = kafkaSslProperties.trustStorePassword
            configProps["ssl.keystore.type"] = "PKCS12"
            configProps["ssl.truststore.type"] = "PKCS12"
            configProps["ssl.keystore.location"] = File(kafkaSslProperties.keyStoreLocation).absolutePath
            configProps["ssl.keystore.password"] = kafkaSslProperties.keyStorePassword
            configProps["ssl.key.password"] = kafkaSslProperties.keyPassword
        }
        return configProps
    }

    companion object {
        const val PARTY_CONSUMER_GROUP_NAME = "party"
    }
}
