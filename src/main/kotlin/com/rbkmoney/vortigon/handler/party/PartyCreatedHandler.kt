package com.rbkmoney.vortigon.handler.party

import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.vortigon.domain.db.enums.Blocking
import com.rbkmoney.vortigon.domain.db.enums.Suspension
import com.rbkmoney.vortigon.domain.db.tables.pojos.Party
import com.rbkmoney.vortigon.handler.ChangeHandler
import com.rbkmoney.vortigon.handler.constant.HandleEventType
import com.rbkmoney.vortigon.repository.PartyDao
import mu.KotlinLogging
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Order(1)
@Component
class PartyCreatedHandler(
    private val partyDao: PartyDao,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        log.debug { "Handle party created change: $change" }
        val partyCreated = change.partyCreated
        val partyCreatedAt = TypeUtil.stringToLocalDateTime(partyCreated.createdAt)
        val party = Party().apply {
            eventId = event.eventId
            eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
            partyId = partyCreated.getId()
            createdAt = partyCreatedAt
            email = partyCreated.contactInfo.getEmail()
            blocking = Blocking.unblocked
            unblockedSince = partyCreatedAt
            suspension = Suspension.active
            suspensionActiveSince = partyCreatedAt
            revisionId = "0"
            revisionChangedAt = partyCreatedAt
        }
        log.debug("Save party create event saveParty: $party")
        partyDao.save(party)
    }

    override val changeType: HandleEventType
        get() = HandleEventType.PARTY_CREATED
}
