package dev.vality.vortigon.handler.party.shop

import dev.vality.damsel.payment_processing.PartyChange
import dev.vality.geck.common.util.TypeUtil
import dev.vality.machinegun.eventsink.MachineEvent
import dev.vality.vortigon.domain.db.enums.Suspension
import dev.vality.vortigon.domain.db.tables.pojos.Shop
import dev.vality.vortigon.handler.ChangeHandler
import dev.vality.vortigon.handler.constant.HandleEventType
import dev.vality.vortigon.handler.merge.BeanNullPropertyMerger
import dev.vality.vortigon.repository.ShopDao
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
        val updateShop = dev.vality.vortigon.domain.db.tables.pojos.Shop().apply {
            this.partyId = partyId
            this.shopId = shopId
            eventId = event.eventId
            eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
            if (shopSuspension.suspension.isSetActive) {
                suspension = dev.vality.vortigon.domain.db.enums.Suspension.active
                suspensionActiveSince = TypeUtil.stringToLocalDateTime(shopSuspension.suspension.active.since)
            } else if (shopSuspension.suspension.isSetSuspended) {
                suspension = dev.vality.vortigon.domain.db.enums.Suspension.suspended
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
