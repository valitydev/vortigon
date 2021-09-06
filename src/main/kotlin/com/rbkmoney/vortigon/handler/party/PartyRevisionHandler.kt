package com.rbkmoney.vortigon.handler.party

import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.vortigon.domain.db.tables.pojos.Party
import com.rbkmoney.vortigon.handler.ChangeHandler
import com.rbkmoney.vortigon.handler.constant.HandleEventType
import com.rbkmoney.vortigon.handler.merge.BeanNullPropertyMerger
import com.rbkmoney.vortigon.repository.PartyDao
import mu.KotlinLogging
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class PartyRevisionHandler(
    private val partyDao: PartyDao,
    private val beanMerger: BeanNullPropertyMerger,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        val partyRevisionChanged = change.revisionChanged
        val party = partyDao.findByPartyId(event.sourceId) ?: Party()
        val updateParty = party.apply {
            partyId = event.sourceId
            eventId = event.eventId
            eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
            revisionId = partyRevisionChanged.revision.toString()
            revisionChangedAt = TypeUtil.stringToLocalDateTime(partyRevisionChanged.timestamp)
        }
        beanMerger.mergeEvent(updateParty, party)
        log.debug("Save party revision change event: $party")
        partyDao.save(party)
    }

    override val changeType: HandleEventType
        get() = HandleEventType.REVISION_CHANGED
}
