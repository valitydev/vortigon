package dev.vality.vortigon.handler.party

import dev.vality.damsel.payment_processing.PartyChange
import dev.vality.geck.common.util.TypeUtil
import dev.vality.machinegun.eventsink.MachineEvent
import dev.vality.vortigon.domain.db.enums.Blocking
import dev.vality.vortigon.domain.db.enums.Suspension
import dev.vality.vortigon.domain.db.tables.pojos.Party
import dev.vality.vortigon.handler.ChangeHandler
import dev.vality.vortigon.handler.constant.HandleEventType
import dev.vality.vortigon.repository.PartyDao
import mu.KotlinLogging
import org.jooq.impl.DSL.domain
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
        val party = dev.vality.vortigon.domain.db.tables.pojos.Party().apply {
            eventId = event.eventId
            eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
            partyId = partyCreated.getId()
            createdAt = partyCreatedAt
            email = partyCreated.contactInfo.getEmail()
            blocking = dev.vality.vortigon.domain.db.enums.Blocking.unblocked
            unblockedSince = partyCreatedAt
            suspension = dev.vality.vortigon.domain.db.enums.Suspension.active
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
