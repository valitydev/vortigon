package com.rbkmoney.vortigon.handler.party.shop

import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.geck.common.util.TBaseUtil
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.vortigon.domain.db.enums.Blocking
import com.rbkmoney.vortigon.domain.db.tables.pojos.Shop
import com.rbkmoney.vortigon.handler.ChangeHandler
import com.rbkmoney.vortigon.handler.constant.HandleEventType
import com.rbkmoney.vortigon.handler.merge.BeanNullPropertyMerger
import com.rbkmoney.vortigon.repository.ShopDao
import mu.KotlinLogging
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class ShopBlockingHandler(
    private val shopDao: ShopDao,
    private val beanMerger: BeanNullPropertyMerger,
) : ChangeHandler<PartyChange, MachineEvent> {

    override fun handleChange(change: PartyChange, event: MachineEvent) {
        log.debug { "Shop account change: $change" }
        val shopId = change.shopBlocking.shopId
        val partyId = event.sourceId
        val updateShop = Shop().apply {
            this.partyId = partyId
            this.shopId = shopId
            this.eventId = event.eventId
            eventTime = TypeUtil.stringToLocalDateTime(event.createdAt)
            blocking = TBaseUtil.unionFieldToEnum(change.shopBlocking.getBlocking(), Blocking::class.java)
            val blocking = change.shopBlocking.blocking
            if (blocking.isSetUnblocked) {
                unblockedReason = blocking.unblocked.reason
                unblockedSince = TypeUtil.stringToLocalDateTime(blocking.unblocked.since)
            } else if (blocking.isSetBlocked) {
                blockedReason = blocking.blocked.reason
                blockedSince = TypeUtil.stringToLocalDateTime(blocking.blocked.since)
            }
        }
        val shop = shopDao.findByPartyIdAndShopId(partyId, shopId)
            ?: throw IllegalStateException("Shop not found. partyId=$partyId;shopId=$shopId")
        beanMerger.mergeEvent(updateShop, shop)
        log.debug { "Save shop: $shop" }
        shopDao.save(shop)
    }

    override val changeType: HandleEventType?
        get() = HandleEventType.SHOP_BLOCKING
}
