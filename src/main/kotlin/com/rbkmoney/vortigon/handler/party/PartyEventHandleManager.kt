package com.rbkmoney.vortigon.handler.party

import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.damsel.payment_processing.PartyEventData
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.sink.common.parser.impl.MachineEventParser
import com.rbkmoney.vortigon.config.properties.KafkaProperties
import com.rbkmoney.vortigon.handler.ChangeHandler
import mu.KotlinLogging
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

private val log = KotlinLogging.logger {}

@Component
class PartyEventHandleManager(
    private val eventParser: MachineEventParser<PartyEventData>,
    private val partyHandlers: List<ChangeHandler<PartyChange, MachineEvent>>,
    private val kafkaProperties: KafkaProperties,
) {

    @Transactional
    fun handleMessages(batch: List<MachineEvent>, ack: Acknowledgment) {
        try {
            if (batch.isEmpty()) return

            for (sinkEvent in batch) {
                handleEvent(sinkEvent)
            }
            ack.acknowledge()
        } catch (e: Exception) {
            log.error(e) { "Exception during PartyListener process" }
            Thread.sleep(kafkaProperties.consumer.throttlingTimeoutMs.toLong())
            throw e
        }
    }

    private fun handleEvent(machineEvent: MachineEvent) {
        log.debug { "Party machine event: $machineEvent" }
        val eventData = eventParser.parse(machineEvent)
        if (eventData.isSetChanges) {
            log.info { "Party changes size: ${eventData.changes.size}" }
            for (change in eventData.getChanges()) {
                log.info { "Party change: $change" }
                partyHandlers.filter { it.accept(change) }
                    .forEach { it.handleChange(change, machineEvent) }
            }
        }
    }
}
