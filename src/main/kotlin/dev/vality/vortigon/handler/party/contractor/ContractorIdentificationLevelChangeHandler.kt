package dev.vality.vortigon.handler.party.contractor

import dev.vality.damsel.payment_processing.PartyChange
import dev.vality.geck.common.util.TypeUtil
import dev.vality.machinegun.eventsink.MachineEvent
import dev.vality.vortigon.domain.db.enums.ContractorIdentificationLvl
import dev.vality.vortigon.domain.db.tables.pojos.Contractor
import dev.vality.vortigon.extension.getClaimStatus
import dev.vality.vortigon.handler.ChangeHandler
import dev.vality.vortigon.handler.constant.HandleEventType
import dev.vality.vortigon.handler.merge.BeanNullPropertyMerger
import dev.vality.vortigon.repository.ContractorDao
import mu.KotlinLogging
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class ContractorIdentificationLevelChangeHandler(
    private val contractorDao: ContractorDao,
    private val beanMerger: BeanNullPropertyMerger,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        log.debug { "Handle contractor identification level change: $change" }
        val claimEffects = change.getClaimStatus()?.accepted?.effects?.filter {
            it.isSetContractorEffect && it.contractorEffect.effect.isSetIdentificationLevelChanged
        }
        claimEffects?.forEach { claimEffect ->
            val contractorEffect = claimEffect.contractorEffect
            val identificationLevelChanged = contractorEffect.effect.identificationLevelChanged
            val contractor =
                contractorDao.findByPartyIdAndContractorId(event.sourceId, contractorEffect.id) ?: dev.vality.vortigon.domain.db.tables.pojos.Contractor()
            val updateContractor = dev.vality.vortigon.domain.db.tables.pojos.Contractor().apply {
                partyId = event.sourceId
                eventId = event.eventId
                eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
                contractorId = contractorEffect.id
                contractorIdentificationLevel = dev.vality.vortigon.domain.db.enums.ContractorIdentificationLvl.valueOf(identificationLevelChanged.name)
            }
            beanMerger.mergeEvent(updateContractor, contractor)
            log.debug { "Save contractor: $contractor" }
            contractorDao.save(contractor)
        }
    }

    override fun accept(change: PartyChange): Boolean {
        if (HandleEventType.CLAIM_CREATED_FILTER.filter.match(change) ||
            HandleEventType.CLAIM_STATUS_CHANGED_FILTER.filter.match(change)
        ) {
            val claimStatus = change.getClaimStatus()
            return claimStatus?.accepted?.effects?.any {
                it.isSetContractorEffect && it.contractorEffect.effect.isSetIdentificationLevelChanged
            } ?: false
        }
        return false
    }
}
