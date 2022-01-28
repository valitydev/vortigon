package dev.vality.vortigon.handler.party

import dev.vality.damsel.payment_processing.PartyChange
import dev.vality.geck.common.util.TBaseUtil
import dev.vality.geck.common.util.TypeUtil
import dev.vality.machinegun.eventsink.MachineEvent
import dev.vality.vortigon.domain.db.enums.Blocking
import dev.vality.vortigon.domain.db.tables.pojos.Party
import dev.vality.vortigon.handler.ChangeHandler
import dev.vality.vortigon.handler.constant.HandleEventType
import dev.vality.vortigon.handler.merge.BeanNullPropertyMerger
import dev.vality.vortigon.repository.PartyDao
import mu.KotlinLogging
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class PartyBlockingHandler(
    private val partyDao: PartyDao,
    private val beanMerger: BeanNullPropertyMerger,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        log.debug { "Handle party blocking change: $change" }
        val partyBlocking = change.partyBlocking
        val party = partyDao.findByPartyId(event.sourceId) ?: Party()
        val updateParty = party.apply {
            partyId = event.sourceId
            eventId = event.eventId
            eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
            blocking = TBaseUtil.unionFieldToEnum(partyBlocking, Blocking::class.java)
            if (partyBlocking.isSetBlocked) {
                blockedReason = partyBlocking.blocked.reason
                blockedSince = TypeUtil.stringToLocalDateTime(partyBlocking.blocked.since)
            } else if (partyBlocking.isSetUnblocked) {
                unblockedReason = partyBlocking.unblocked.reason
                unblockedSince = TypeUtil.stringToLocalDateTime(partyBlocking.unblocked.since)
            }
        }
        beanMerger.mergeEvent(updateParty, party)
        log.debug("Save party blocking event: $party")
        partyDao.save(party)
    }

    override val changeType: HandleEventType
        get() = HandleEventType.PARTY_BLOCKING
}
