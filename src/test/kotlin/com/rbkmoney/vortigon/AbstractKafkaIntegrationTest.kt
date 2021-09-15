package com.rbkmoney.vortigon

import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.damsel.payment_processing.PartyEventData
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.kafka.common.serialization.ThriftSerializer
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.machinegun.eventsink.SinkEvent
import com.rbkmoney.machinegun.msgpack.Value
import com.rbkmoney.vortigon.serializer.MachineEventDeserializer
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.test.util.TestPropertyValues
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ContextConfiguration
import org.testcontainers.containers.KafkaContainer
import java.time.Duration
import java.time.Instant
import java.util.Properties

private val log = KotlinLogging.logger {}

@DirtiesContext
@ContextConfiguration(initializers = [AbstractKafkaIntegrationTest.Initializer::class])
abstract class AbstractKafkaIntegrationTest : PostgresAbstractTest() {

    protected fun createMachineEvent(partyChange: PartyChange, sourceId: String, sequenceId: Long): MachineEvent {
        val message = MachineEvent()
        val payload = PartyEventData()
        val partyChanges: ArrayList<PartyChange> = ArrayList<PartyChange>()
        partyChanges.add(partyChange)
        payload.setChanges(partyChanges)
        message.createdAt = TypeUtil.temporalToString(Instant.now())
        message.eventId = sequenceId
        message.sourceNs = "sda"
        message.sourceId = sourceId
        val eventPayloadThriftSerializer: ThriftSerializer<PartyEventData> = ThriftSerializer<PartyEventData>()
        val data = Value()
        payload.validate()
        data.setBin(eventPayloadThriftSerializer.serialize("", payload))
        message.setData(data)
        return message
    }

    protected fun createSinkEvent(machineEvent: MachineEvent): SinkEvent {
        val sinkEvent = SinkEvent()
        sinkEvent.event = machineEvent
        return sinkEvent
    }

    protected fun <T> createProducer(): Producer<String, T> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        props[ProducerConfig.CLIENT_ID_CONFIG] = "CLIENT"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ThriftSerializer::class.java
        return KafkaProducer<String, T>(props)
    }

    object Initializer : ApplicationContextInitializer<ConfigurableApplicationContext> {
        override fun initialize(configurableApplicationContext: ConfigurableApplicationContext) {
            kafka.start()
            val topicName = "mg-events-party"
            TestPropertyValues
                .of(
                    "kafka.bootstrap-servers=${kafka.bootstrapServers}",
                    "kafka.topics.party-shop.id=$topicName"
                )
                .applyTo(configurableApplicationContext.environment)
            initTopic<Any>(topicName)
        }

        private fun <T> initTopic(topicName: String) {
            val consumer = createConsumer<T>(
                MachineEventDeserializer::class.java
            )
            try {
                consumer.subscribe(listOf(topicName))
                consumer.poll(Duration.ofMillis(100L))
            } catch (e: Exception) {
                log.error(e) { "Error during initialize topic $topicName" }
            }
            consumer.close()
        }

        fun <T> createConsumer(clazz: Class<*>): Consumer<String, T> {
            val props = Properties()
            props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
            props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
            props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = clazz
            props[ConsumerConfig.GROUP_ID_CONFIG] = "test"
            props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
            return KafkaConsumer<String, T>(props)
        }
    }

    companion object {
        private val kafka: KafkaContainer by lazy {
            KafkaContainer(KAFKA_DOCKER_VERSION).withEmbeddedZookeeper()
        }
        private const val KAFKA_DOCKER_VERSION = "5.0.1"
    }
}
