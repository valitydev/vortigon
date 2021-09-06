package com.rbkmoney.vortigon.handler.party

import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.geck.common.util.TBaseUtil
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.vortigon.domain.db.enums.Blocking
import com.rbkmoney.vortigon.domain.db.tables.pojos.Party
import com.rbkmoney.vortigon.handler.ChangeHandler
import com.rbkmoney.vortigon.handler.constant.HandleEventType
import com.rbkmoney.vortigon.handler.merge.BeanNullPropertyMerger
import com.rbkmoney.vortigon.repository.PartyDao
import mu.KotlinLogging
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class PartyBlockingHandler(
    private val partyDao: PartyDao,
    private val beanMerger: BeanNullPropertyMerger,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
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
