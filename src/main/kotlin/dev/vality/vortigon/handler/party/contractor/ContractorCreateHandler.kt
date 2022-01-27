package dev.vality.vortigon.handler.party.contractor

import dev.vality.damsel.payment_processing.PartyChange
import dev.vality.geck.common.util.TypeUtil
import dev.vality.machinegun.eventsink.MachineEvent
import dev.vality.vortigon.domain.db.enums.ContractorIdentificationLvl
import dev.vality.vortigon.domain.db.tables.pojos.Contractor
import dev.vality.vortigon.extension.getClaimStatus
import dev.vality.vortigon.handler.ChangeHandler
import dev.vality.vortigon.handler.constant.HandleEventType
import dev.vality.vortigon.repository.ContractorDao
import mu.KotlinLogging
import org.springframework.core.annotation.Order
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Order(2)
@Component
class ContractorCreateHandler(
    private val contractorDao: ContractorDao,
    private val conversionService: ConversionService,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        log.debug { "Handle contractor create change: $change" }
        val claimEffects = change.getClaimStatus()?.accepted?.effects?.filter {
            it.isSetContractorEffect && it.contractorEffect.effect.isSetCreated
        }
        claimEffects?.forEach { claimEffect ->
            log.debug { "Contractor create change. Handle effect: $claimEffect" }
            val contractorEffect = claimEffect.contractorEffect
            val partyContractor = contractorEffect.effect.created
            val contractor = partyContractor.contractor
            val contractorDomain = conversionService.convert(contractor, dev.vality.vortigon.domain.db.tables.pojos.Contractor::class.java)!!.apply {
                partyId = event.sourceId
                eventId = event.eventId
                eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
                contractorId = contractorEffect.id
                contractorIdentificationLevel = dev.vality.vortigon.domain.db.enums.ContractorIdentificationLvl.valueOf(partyContractor.status.name)
            }
            log.debug { "Save contractor: $contractor" }
            contractorDao.save(contractorDomain)
        }
    }

    override fun accept(change: PartyChange): Boolean {
        if (HandleEventType.CLAIM_CREATED_FILTER.filter.match(change) ||
            HandleEventType.CLAIM_STATUS_CHANGED_FILTER.filter.match(change)
        ) {
            val claimStatus = change.getClaimStatus()
            return claimStatus?.accepted?.effects?.any {
                it.isSetContractorEffect && it.contractorEffect.effect.isSetCreated
            } ?: false
        }
        return false
    }
}
