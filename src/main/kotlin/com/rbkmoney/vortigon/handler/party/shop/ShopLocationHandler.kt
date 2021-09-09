package com.rbkmoney.vortigon.handler.party.shop

import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.vortigon.domain.db.tables.pojos.Shop
import com.rbkmoney.vortigon.extension.getClaimStatus
import com.rbkmoney.vortigon.handler.ChangeHandler
import com.rbkmoney.vortigon.handler.constant.HandleEventType
import com.rbkmoney.vortigon.handler.merge.BeanNullPropertyMerger
import com.rbkmoney.vortigon.repository.ShopDao
import mu.KotlinLogging
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class ShopLocationHandler(
    private val shopDao: ShopDao,
    private val beanMerger: BeanNullPropertyMerger,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        log.debug { "Handle shop location change: $change" }
        val claimEffects = change.getClaimStatus()?.accepted?.effects
        claimEffects?.filter {
            it.isSetShopEffect && it.shopEffect.effect.isSetLocationChanged
        }?.forEach { claimEffect ->
            val shopEffectUnit = claimEffect.shopEffect
            val locationChanged = shopEffectUnit.effect.locationChanged
            val shopId: String = claimEffect.shopEffect.shopId
            val partyId = event.sourceId

            val shop = shopDao.findByPartyIdAndShopId(partyId, shopId)
                ?: throw IllegalStateException("Shop not found. partyId=$partyId; shopId=$shopId")
            val updateShop = Shop().apply {
                this.partyId = partyId
                this.shopId = shopId
                eventId = event.eventId
                eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
                locationUrl = locationChanged.url
            }
            beanMerger.mergeEvent(updateShop, shop)
            shopDao.save(shop)
        }
    }

    override fun accept(change: PartyChange): Boolean {
        if (HandleEventType.CLAIM_CREATED_FILTER.filter.match(change) ||
            HandleEventType.CLAIM_STATUS_CHANGED_FILTER.filter.match(change)
        ) {
            return change.getClaimStatus()?.accepted?.effects?.any {
                it.isSetShopEffect && it.shopEffect.effect.isSetLocationChanged
            } == true
        }
        return false
    }
}
