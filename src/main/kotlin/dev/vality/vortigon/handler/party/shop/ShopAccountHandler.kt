package dev.vality.vortigon.handler.party.shop

import dev.vality.damsel.payment_processing.PartyChange
import dev.vality.geck.common.util.TypeUtil
import dev.vality.machinegun.eventsink.MachineEvent
import dev.vality.vortigon.domain.db.tables.pojos.Shop
import dev.vality.vortigon.extension.getClaimStatus
import dev.vality.vortigon.handler.ChangeHandler
import dev.vality.vortigon.handler.constant.HandleEventType
import dev.vality.vortigon.handler.merge.BeanNullPropertyMerger
import dev.vality.vortigon.repository.ShopDao
import mu.KotlinLogging
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class ShopAccountHandler(
    private val shopDao: ShopDao,
    private val beanMerger: BeanNullPropertyMerger,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        log.debug { "Handle shop account change: $change" }
        val claimEffects = change.getClaimStatus()?.accepted?.effects
        claimEffects?.filter {
            it.isSetShopEffect && it.shopEffect.effect.isSetAccountCreated
        }?.forEach { claimEffect ->
            val shopEffectUnit = claimEffect.shopEffect
            val accountCreated = shopEffectUnit.effect.accountCreated
            val shopId = shopEffectUnit.shop_id
            val partyId = event.sourceId

            val shop = shopDao.findByPartyIdAndShopId(partyId, shopId)
                ?: throw IllegalStateException("Shop not found. partyId=$partyId; shopId=$shopId")
            val updateShop = Shop().apply {
                this.partyId = partyId
                this.shopId = shopId
                eventId = event.eventId
                eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
                accountCurrencyCode = accountCreated.getCurrency().symbolicCode
                accountGuarantee = accountCreated.getGuarantee().toString()
                accountSettlement = accountCreated.getSettlement().toString()
                accountPayout = accountCreated.getPayout().toString()
            }
            println(updateShop)
            beanMerger.mergeEvent(updateShop, shop)
            log.debug { "Save shop: $shop" }
            shopDao.save(shop)
        }
    }

    override fun accept(change: PartyChange): Boolean {
        if (HandleEventType.CLAIM_CREATED_FILTER.filter.match(change) ||
            HandleEventType.CLAIM_STATUS_CHANGED_FILTER.filter.match(change)
        ) {
            return change.getClaimStatus()?.accepted?.effects?.any {
                it.isSetShopEffect && it.shopEffect.effect.isSetAccountCreated
            } == true
        }
        return false
    }
}
