package dev.vality.vortigon.handler.party

import dev.vality.damsel.payment_processing.PartyChange
import dev.vality.damsel.payment_processing.PartyEventData
import dev.vality.machinegun.eventsink.MachineEvent
import dev.vality.sink.common.parser.impl.MachineEventParser
import dev.vality.vortigon.handler.ChangeHandler
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

private val log = KotlinLogging.logger {}

@Component
class PartyEventHandleManager(
    private val eventParser: MachineEventParser<PartyEventData>,
    private val partyHandlers: List<ChangeHandler<PartyChange, MachineEvent>>,
    @Value("\${kafka.consumer.throttling-timeout-ms}")
    private val throttlingTimeoutMs: Int,
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
            Thread.sleep(throttlingTimeoutMs.toLong())
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
