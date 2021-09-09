package com.rbkmoney.vortigon.handler.party.contractor

import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.vortigon.domain.db.enums.ContractorIdentificationLvl
import com.rbkmoney.vortigon.domain.db.tables.pojos.Contractor
import com.rbkmoney.vortigon.extension.getClaimStatus
import com.rbkmoney.vortigon.handler.ChangeHandler
import com.rbkmoney.vortigon.handler.constant.HandleEventType
import com.rbkmoney.vortigon.repository.ContractorDao
import mu.KotlinLogging
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

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
            val contractorDomain = conversionService.convert(contractor, Contractor::class.java)!!.apply {
                partyId = event.sourceId
                eventId = event.eventId
                eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
                contractorId = contractorEffect.id
                contractorIdentificationLevel = ContractorIdentificationLvl.valueOf(partyContractor.status.name)
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
