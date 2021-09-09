package com.rbkmoney.vortigon.handler.party.shop

import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.vortigon.domain.db.enums.Suspension
import com.rbkmoney.vortigon.domain.db.tables.pojos.Shop
import com.rbkmoney.vortigon.handler.ChangeHandler
import com.rbkmoney.vortigon.handler.constant.HandleEventType
import com.rbkmoney.vortigon.handler.merge.BeanNullPropertyMerger
import com.rbkmoney.vortigon.repository.ShopDao
import mu.KotlinLogging
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class ShopSuspensionHandler(
    private val shopDao: ShopDao,
    private val beanMerger: BeanNullPropertyMerger,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        log.debug { "Handle shop suspension change: $change" }
        val shopSuspension = change.shopSuspension
        val shopId = shopSuspension.shopId
        val partyId = event.sourceId

        val shop = shopDao.findByPartyIdAndShopId(partyId, shopId)
            ?: throw IllegalStateException("Shop not found. partyId=$partyId; shopId=$shopId")
        val updateShop = Shop().apply {
            this.partyId = partyId
            this.shopId = shopId
            eventId = event.eventId
            eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
            if (shopSuspension.suspension.isSetActive) {
                suspension = Suspension.active
                suspensionActiveSince = TypeUtil.stringToLocalDateTime(shopSuspension.suspension.active.since)
            } else if (shopSuspension.suspension.isSetSuspended) {
                suspension = Suspension.suspended
                suspensionSuspendedSince = TypeUtil.stringToLocalDateTime(shopSuspension.suspension.suspended.since)
            }
        }
        beanMerger.mergeEvent(updateShop, shop)
        log.debug { "Save shop: $shop" }
        shopDao.save(shop)
    }

    override val changeType: HandleEventType
        get() = HandleEventType.SHOP_SUSPENSION
}
