package com.rbkmoney.vortigon.handler.party

import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.geck.common.util.TBaseUtil
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.vortigon.domain.db.enums.Suspension
import com.rbkmoney.vortigon.domain.db.tables.pojos.Party
import com.rbkmoney.vortigon.handler.ChangeHandler
import com.rbkmoney.vortigon.handler.constant.HandleEventType
import com.rbkmoney.vortigon.handler.merge.BeanNullPropertyMerger
import com.rbkmoney.vortigon.repository.PartyDao
import mu.KotlinLogging
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class PartySuspensionHandler(
    private val partyDao: PartyDao,
    private val beanMerger: BeanNullPropertyMerger,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        val partySuspension = change.partySuspension
        val party = partyDao.findByPartyId(event.sourceId) ?: Party()
        val updateParty = party.apply {
            partyId = event.sourceId
            eventId = event.eventId
            eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
            suspension = TBaseUtil.unionFieldToEnum(partySuspension, Suspension::class.java)
            if (partySuspension.isSetActive) {
                suspensionActiveSince = TypeUtil.stringToLocalDateTime(partySuspension.active.since)
            } else if (partySuspension.isSetSuspended) {
                suspensionSuspendedSince = TypeUtil.stringToLocalDateTime(partySuspension.suspended.since)
            }
        }
        beanMerger.mergeEvent(updateParty, party)
        log.debug("Save party suspension event: $party")
        partyDao.save(party)
    }

    override val changeType: HandleEventType
        get() = HandleEventType.PARTY_SUSPENSION
}
