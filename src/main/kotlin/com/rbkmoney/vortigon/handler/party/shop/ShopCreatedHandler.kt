package com.rbkmoney.vortigon.handler.party.shop

import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.geck.common.util.TBaseUtil
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.vortigon.domain.db.enums.Blocking
import com.rbkmoney.vortigon.domain.db.tables.pojos.Shop
import com.rbkmoney.vortigon.extension.getClaimStatus
import com.rbkmoney.vortigon.handler.ChangeHandler
import com.rbkmoney.vortigon.handler.constant.HandleEventType
import com.rbkmoney.vortigon.repository.ContractDao
import com.rbkmoney.vortigon.repository.ContractorDao
import com.rbkmoney.vortigon.repository.ShopDao
import mu.KotlinLogging
import org.springframework.core.annotation.Order
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Order(4)
@Component
class ShopCreatedHandler(
    private val shopDao: ShopDao,
    private val contractDao: ContractDao,
    private val contractorDao: ContractorDao,
    private val conversionService: ConversionService,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        log.debug { "Handle shop create change: $change" }
        val effects = change.getClaimStatus()?.accepted?.effects
        effects?.filter {
            it.isSetShopEffect && it.shopEffect.effect.isSetCreated
        }?.forEach { effect ->
            val shopEffect = effect.shopEffect
            val shopCreated = shopEffect.effect.created
            val shopId = shopEffect.shopId
            val partyId = event.sourceId
            val contract = contractDao.findByPartyIdAndContractId(partyId, shopCreated.contractId)
                ?: throw IllegalStateException("Contract not found. partyId=$partyId; contractId=${shopCreated.contractId}")
            val contractor = contractorDao.findByPartyIdAndContractorId(partyId, contract.contractorId)
            val shop = conversionService.convert(contractor, Shop::class.java)!!.apply {
                eventId = event.eventId
                eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
                this.shopId = shopId
                this.partyId = partyId
                createdAt = TypeUtil.stringToLocalDateTime(shopCreated.createdAt)
                blocking = TBaseUtil.unionFieldToEnum(shopCreated.blocking, Blocking::class.java)
                if (shopCreated.getBlocking().isSetUnblocked) {
                    unblockedReason = shopCreated.blocking.unblocked.reason
                    unblockedSince = TypeUtil.stringToLocalDateTime(shopCreated.blocking.unblocked.since)
                } else if (shopCreated.getBlocking().isSetBlocked) {
                    blockedReason = shopCreated.blocking.blocked.reason
                    blockedSince = TypeUtil.stringToLocalDateTime(shopCreated.blocking.blocked.since)
                }
                detailsName = shopCreated.details.name
                detailsDescription = shopCreated.details.description
                if (shopCreated.getLocation().isSetUrl) {
                    locationUrl = shopCreated.location.url
                }
                categoryId = shopCreated.category.id
                if (shopCreated.isSetAccount) {
                    accountCurrencyCode = shopCreated.account.currency.symbolicCode
                    accountGuarantee = shopCreated.account.guarantee.toString()
                    accountPayout = shopCreated.account.payout.toString()
                    accountSettlement = shopCreated.account.settlement.toString()
                }
                contractId = contract.contractId
                payoutToolId = shopCreated.payoutToolId
                if (shopCreated.isSetPayoutSchedule) {
                    payoutScheduleId = shopCreated.payoutSchedule.id
                }
            }
            log.debug { "Save shop: $shop" }
            shopDao.save(shop)
        }
    }

    override fun accept(change: PartyChange): Boolean {
        if (HandleEventType.CLAIM_CREATED_FILTER.filter.match(change) ||
            HandleEventType.CLAIM_STATUS_CHANGED_FILTER.filter.match(change)
        ) {
            return change.getClaimStatus()?.accepted?.effects?.any {
                it.isSetShopEffect && it.shopEffect.effect.isSetCreated
            } == true
        }
        return false
    }
}
