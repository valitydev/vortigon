package com.rbkmoney.vortigon.listener

import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.vortigon.handler.party.PartyEventHandleManager
import mu.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
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
        batch: List<MachineEvent>,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partition: Int,
        @Header(KafkaHeaders.OFFSET) offsets: Int,
        ack: Acknowledgment,
    ) {
        log.info { "PartyListener listen offsets=$offsets partition=$partition batch.size=${batch.size}" }
        partyEventHandleManager.handleMessages(batch, ack)
        log.info { "PartyListener batch has been committed, batch.size=${batch.size}" }
    }
}
