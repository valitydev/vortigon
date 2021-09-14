package com.rbkmoney.vortigon.handler.party.contract

import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.vortigon.domain.db.tables.pojos.Contract
import com.rbkmoney.vortigon.domain.db.tables.pojos.Shop
import com.rbkmoney.vortigon.extension.getClaimStatus
import com.rbkmoney.vortigon.handler.ChangeHandler
import com.rbkmoney.vortigon.handler.constant.HandleEventType
import com.rbkmoney.vortigon.handler.merge.BeanNullPropertyMerger
import com.rbkmoney.vortigon.repository.ContractDao
import com.rbkmoney.vortigon.repository.ContractorDao
import com.rbkmoney.vortigon.repository.ShopDao
import mu.KotlinLogging
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class ContractContractorIdChangeHandler(
    private val contractDao: ContractDao,
    private val contractorDao: ContractorDao,
    private val shopDao: ShopDao,
    private val beanMerger: BeanNullPropertyMerger,
    private val conversionService: ConversionService,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        log.debug { "Handle contract contractor id change: $change" }
        val claimEffects = change.getClaimStatus()?.accepted?.effects?.filter {
            it.isSetContractEffect && it.contractEffect.effect.isSetContractorChanged
        }
        claimEffects?.forEach { claimEffect ->
            val contractEffect = claimEffect.contractEffect
            val contract = contractDao.findByPartyIdAndContractId(
                event.sourceId,
                contractEffect.contractId
            ) ?: Contract()
            val updateContract = Contract().apply {
                partyId = event.sourceId
                contractId = contractEffect.contractId
                contractorId = contractEffect.effect.contractorChanged
                eventId = event.eventId
                eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
            }
            beanMerger.mergeEvent(updateContract, contract)
            contractDao.save(contract)
            updateShops(event, contract.contractorId, contract.contractId, contractEffect.effect.contractorChanged)
        }
    }

    fun updateShops(
        event: MachineEvent,
        contractorId: String,
        oldContractId: String,
        newContractId: String,
    ) {
        log.debug { "Update shops with partyId=${event.sourceId}; contractId=$oldContractId" }
        val currentShopStates = shopDao.findByPartyIdAndContractId(event.sourceId, oldContractId)
        log.debug { "Update shops: $currentShopStates" }
        if (currentShopStates?.isNotEmpty() == true) {
            val currentContractorState = contractorDao.findByPartyIdAndContractorId(event.sourceId, contractorId)
            val newShop = conversionService.convert(currentContractorState, Shop::class.java)!!
            val shopList = currentShopStates.map { shop ->
                beanMerger.mergeEvent(newShop, shop)
                shop.eventId = event.eventId
                shop.eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
                shop
            }
            log.debug { "Save shops: $shopList" }
            shopDao.saveAll(shopList)
        }
    }

    override fun accept(change: PartyChange): Boolean {
        if (HandleEventType.CLAIM_CREATED_FILTER.filter.match(change) ||
            HandleEventType.CLAIM_STATUS_CHANGED_FILTER.filter.match(change)
        ) {
            val claimStatus = change.getClaimStatus()
            return claimStatus?.accepted?.effects?.any {
                it.isSetContractEffect && it.contractEffect.effect.isSetContractorChanged
            } ?: false
        }
        return false
    }
}
