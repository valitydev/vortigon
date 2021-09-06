package com.rbkmoney.vortigon.listener

import com.rbkmoney.machinegun.eventsink.SinkEvent
import com.rbkmoney.vortigon.handler.party.PartyEventHandleManager
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class PartyManagementListener(
    private val partyEventHandleManager: PartyEventHandleManager,
) {

    @KafkaListener(
        autoStartup = "\${kafka.consumer.enabled}",
        topics = ["\${kafka.topic.party.initial}"],
        containerFactory = "partyListenerContainerFactory"
    )
    fun handle(
        messages: List<ConsumerRecord<String, SinkEvent>>,
        ack: Acknowledgment,
    ) {
        log.info { "PartyListener listen machineEvent batch with size: $messages.size" }
        val events = messages.map { it.value().event }
        partyEventHandleManager.handleMessages(events, ack)
        log.info { "PartyListener batch has been committed, batch.size=${messages.size}" }
    }
}
