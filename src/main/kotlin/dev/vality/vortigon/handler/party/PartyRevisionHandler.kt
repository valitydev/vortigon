package dev.vality.vortigon.handler.party

import dev.vality.damsel.payment_processing.PartyChange
import dev.vality.geck.common.util.TypeUtil
import dev.vality.machinegun.eventsink.MachineEvent
import dev.vality.vortigon.domain.db.tables.pojos.Party
import dev.vality.vortigon.handler.ChangeHandler
import dev.vality.vortigon.handler.constant.HandleEventType
import dev.vality.vortigon.handler.merge.BeanNullPropertyMerger
import dev.vality.vortigon.repository.PartyDao
import mu.KotlinLogging
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class PartyRevisionHandler(
    private val partyDao: PartyDao,
    private val beanMerger: BeanNullPropertyMerger,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        log.debug { "Handle party revision change: $change" }
        val partyRevisionChanged = change.revisionChanged
        val party = partyDao.findByPartyId(event.sourceId) ?: dev.vality.vortigon.domain.db.tables.pojos.Party()
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
