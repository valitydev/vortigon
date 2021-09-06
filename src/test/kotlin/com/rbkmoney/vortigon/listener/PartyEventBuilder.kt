package com.rbkmoney.vortigon.listener

import com.rbkmoney.damsel.domain.Active
import com.rbkmoney.damsel.domain.Blocked
import com.rbkmoney.damsel.domain.Blocking
import com.rbkmoney.damsel.domain.BusinessScheduleRef
import com.rbkmoney.damsel.domain.CategoryRef
import com.rbkmoney.damsel.domain.Contract
import com.rbkmoney.damsel.domain.ContractActive
import com.rbkmoney.damsel.domain.ContractStatus
import com.rbkmoney.damsel.domain.Contractor
import com.rbkmoney.damsel.domain.ContractorIdentificationLevel
import com.rbkmoney.damsel.domain.CurrencyRef
import com.rbkmoney.damsel.domain.LegalEntity
import com.rbkmoney.damsel.domain.PartyContactInfo
import com.rbkmoney.damsel.domain.PartyContractor
import com.rbkmoney.damsel.domain.Shop
import com.rbkmoney.damsel.domain.ShopAccount
import com.rbkmoney.damsel.domain.ShopDetails
import com.rbkmoney.damsel.domain.ShopLocation
import com.rbkmoney.damsel.domain.Suspended
import com.rbkmoney.damsel.domain.Suspension
import com.rbkmoney.damsel.domain.TermSetHierarchyRef
import com.rbkmoney.damsel.domain.Unblocked
import com.rbkmoney.damsel.payment_processing.Claim
import com.rbkmoney.damsel.payment_processing.ClaimAccepted
import com.rbkmoney.damsel.payment_processing.ClaimEffect
import com.rbkmoney.damsel.payment_processing.ClaimStatus
import com.rbkmoney.damsel.payment_processing.ContractEffect
import com.rbkmoney.damsel.payment_processing.ContractEffectUnit
import com.rbkmoney.damsel.payment_processing.ContractorEffect
import com.rbkmoney.damsel.payment_processing.ContractorEffectUnit
import com.rbkmoney.damsel.payment_processing.PartyChange
import com.rbkmoney.damsel.payment_processing.PartyCreated
import com.rbkmoney.damsel.payment_processing.PartyEventData
import com.rbkmoney.damsel.payment_processing.PartyRevisionChanged
import com.rbkmoney.damsel.payment_processing.ScheduleChanged
import com.rbkmoney.damsel.payment_processing.ShopBlocking
import com.rbkmoney.damsel.payment_processing.ShopEffect
import com.rbkmoney.damsel.payment_processing.ShopEffectUnit
import com.rbkmoney.damsel.payment_processing.ShopSuspension
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.geck.serializer.kit.mock.MockMode
import com.rbkmoney.geck.serializer.kit.mock.MockTBaseProcessor
import com.rbkmoney.geck.serializer.kit.tbase.TBaseHandler
import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.machinegun.eventsink.SinkEvent
import com.rbkmoney.machinegun.msgpack.Value
import com.rbkmoney.sink.common.serialization.impl.PartyEventDataSerializer
import io.github.benas.randombeans.api.EnhancedRandom
import java.time.Instant
import java.time.LocalDateTime
import java.util.UUID

object PartyEventBuilder {

    fun buildPartyCreatedPartyChange(partyId: String): PartyChange {
        val partyCreated: PartyCreated = buildPartyCreated(partyId)
        return PartyChange().apply { this.partyCreated = partyCreated }
    }

    fun buildPartyCreated(partyId: String): PartyCreated {
        return PartyCreated(
            partyId,
            PartyContactInfo(EnhancedRandom.random(String::class.java)),
            TypeUtil.temporalToString(LocalDateTime.now())
        )
    }

    fun buildPartyBlockingPartyChange(): PartyChange {
        val blocking: Blocking = buildPartyBlocking()
        return PartyChange().apply { partyBlocking = blocking }
    }

    fun buildPartyBlocking(): Blocking {
        val blocking = Blocking().apply {
            blocked = Blocked("testBlockedReason", TypeUtil.temporalToString(LocalDateTime.now()))
        }
        return blocking
    }

    fun buildPartyRevisionChangedPartyChange(): PartyChange {
        val partyRevisionChanged: PartyRevisionChanged = buildPartyRevisionChanged()
        val partyChange = PartyChange().apply {
            revisionChanged = partyRevisionChanged
        }
        return partyChange
    }

    fun buildPartyRevisionChanged(): PartyRevisionChanged {
        return PartyRevisionChanged(
            TypeUtil.temporalToString(LocalDateTime.now()),
            4324L
        )
    }

    fun buildPartySuspensionPartyChange(): PartyChange {
        val suspension: Suspension = buildActiveSuspension()
        val partyChange = PartyChange().apply {
            partySuspension = suspension
        }
        return partyChange
    }

    fun buildActiveSuspension(): Suspension {
        val suspension = Suspension().apply {
            active = Active(TypeUtil.temporalToString(LocalDateTime.now()))
        }
        return suspension
    }

    fun buildShopCreatedPartyChange(shopId: String): PartyChange {
        val shopEffectUnit = ShopEffectUnit()
        shopEffectUnit.shopId = shopId
        val shopEffect = ShopEffect()
        shopEffect.created = buildShopCreated()
        shopEffectUnit.setEffect(shopEffect)
        val claimEffect = ClaimEffect()
        claimEffect.shopEffect = shopEffectUnit
        val claim: Claim = buildClaimCreated(claimEffect)
        val partyChange = PartyChange()
        partyChange.claimCreated = claim
        return partyChange
    }

    fun buildShopCreated(): Shop {
        var shop = Shop()
        shop = MockTBaseProcessor(MockMode.ALL).process(shop, TBaseHandler(Shop::class.java))
        shop.createdAt = TypeUtil.temporalToString(LocalDateTime.now())
        val blocking = Blocking().apply {
            unblocked = Unblocked("testReason", TypeUtil.temporalToString(LocalDateTime.now()))
        }
        shop.setBlocking(blocking)
        shop.contractId = UUID.randomUUID().toString()
        shop.setSuspension(buildActiveSuspension())
        return shop
    }

    fun buildShopBlockingPartyChange(shopId: String): PartyChange {
        val shopBlocking: ShopBlocking = buildShopBlocking(shopId)
        val partyChange = PartyChange()
        partyChange.shopBlocking = shopBlocking
        return partyChange
    }

    fun buildShopBlocking(shopId: String): ShopBlocking {
        val blocking = Blocking().apply {
            unblocked = Unblocked("testReason", TypeUtil.temporalToString(LocalDateTime.now()))
        }
        return ShopBlocking(shopId, blocking)
    }

    fun buildShopCategoryPartyChange(shopId: String, categoryId: Int): PartyChange {
        val shopEffectUnit = ShopEffectUnit()
        shopEffectUnit.shopId = shopId
        val shopEffect = ShopEffect()
        val categoryRef = CategoryRef()
        categoryRef.setId(categoryId)
        shopEffect.categoryChanged = categoryRef
        shopEffectUnit.setEffect(shopEffect)
        val claimEffect = ClaimEffect()
        claimEffect.shopEffect = shopEffectUnit
        val claim: Claim = buildClaimCreated(claimEffect)
        val partyChange = PartyChange()
        partyChange.claimCreated = claim
        return partyChange
    }

    fun buildShopAccountCreatedPartyChange(shopdId: String): PartyChange {
        val shopEffectUnit = ShopEffectUnit()
        shopEffectUnit.shopId = shopdId
        val shopAccount = ShopAccount()
        shopAccount.setCurrency(CurrencyRef("RUB"))
        shopAccount.setPayout(EnhancedRandom.random(Long::class.java))
        shopAccount.setSettlement(EnhancedRandom.random(Long::class.java))
        val shopEffect = ShopEffect()
        shopEffect.accountCreated = shopAccount
        shopEffectUnit.setEffect(shopEffect)
        val claimEffect = ClaimEffect()
        claimEffect.shopEffect = shopEffectUnit
        val claim: Claim = buildClaimCreated(claimEffect)
        val partyChange = PartyChange()
        partyChange.claimCreated = claim
        return partyChange
    }

    fun buildShopDetailsPartyChange(shopId: String): PartyChange {
        val shopEffectUnit = ShopEffectUnit()
        shopEffectUnit.shopId = shopId
        val shopDetails = ShopDetails()
        shopDetails.setName("testDetailsName")
        shopDetails.setDescription("testDetailsDescription")
        val shopEffect = ShopEffect()
        shopEffect.detailsChanged = shopDetails
        shopEffectUnit.setEffect(shopEffect)
        val claimEffect = ClaimEffect()
        claimEffect.shopEffect = shopEffectUnit
        val claim: Claim = buildClaimCreated(claimEffect)
        val partyChange = PartyChange()
        partyChange.claimCreated = claim
        return partyChange
    }

    fun buildShopLocationPartyChange(shopId: String): PartyChange {
        val shopEffectUnit = ShopEffectUnit()
        shopEffectUnit.shopId = shopId
        val shopLocation = ShopLocation().apply {
            url = "testUrl"
        }
        val shopEffect = ShopEffect()
        shopEffect.locationChanged = shopLocation
        shopEffectUnit.setEffect(shopEffect)
        val claimEffect = ClaimEffect()
        claimEffect.shopEffect = shopEffectUnit
        val claim: Claim = buildClaimCreated(claimEffect)
        val partyChange = PartyChange()
        partyChange.claimCreated = claim
        return partyChange
    }

    fun buildShopPayoutScheduleChangedPartyChange(shopdId: String): PartyChange {
        val shopEffectUnit = ShopEffectUnit()
        shopEffectUnit.shopId = shopdId
        val scheduleChanged = ScheduleChanged()
        scheduleChanged.setSchedule(BusinessScheduleRef(EnhancedRandom.random(Int::class.java)))
        val shopEffect = ShopEffect()
        shopEffect.payoutScheduleChanged = scheduleChanged
        shopEffectUnit.setEffect(shopEffect)
        val claimEffect = ClaimEffect()
        claimEffect.shopEffect = shopEffectUnit
        val claim: Claim = buildClaimCreated(claimEffect)
        val partyChange = PartyChange()
        partyChange.claimCreated = claim
        return partyChange
    }

    fun buildShopPayoutToolChangedPartyChange(shopdId: String, payoutToolId: String): PartyChange {
        val shopEffectUnit = ShopEffectUnit()
        shopEffectUnit.shopId = shopdId
        val shopEffect = ShopEffect()
        shopEffect.payoutToolChanged = payoutToolId
        shopEffectUnit.setEffect(shopEffect)
        val claimEffect = ClaimEffect()
        claimEffect.shopEffect = shopEffectUnit
        val claim: Claim = buildClaimCreated(claimEffect)
        val partyChange = PartyChange()
        partyChange.claimCreated = claim
        return partyChange
    }

    fun buildContract(contractId: String): Contract {
        val contract = Contract()
        var contractor: Contractor = Contractor()
        contractor = MockTBaseProcessor(MockMode.ALL).process(
            contractor,
            TBaseHandler(
                Contractor::class.java
            )
        )
        contract.setContractor(contractor)
        contract.setId(contractId)
        contract.createdAt = TypeUtil.temporalToString(LocalDateTime.now())
        contract.validSince = TypeUtil.temporalToString(LocalDateTime.now())
        contract.validUntil = TypeUtil.temporalToString(LocalDateTime.now().plusYears(2))
        contract.setStatus(ContractStatus.active(ContractActive()))
        contract.setTerms(TermSetHierarchyRef())
        contract.setAdjustments(listOf())
        contract.payoutTools = listOf()
        contract.contractorId = null
        return contract
    }

    fun buildContractCreatedPartyChange(contract: Contract): PartyChange {
        val contractEffectUnit = ContractEffectUnit()
        contractEffectUnit.contractId = contract.getId()
        val contractEffect = ContractEffect()
        contractEffect.created = contract
        contractEffectUnit.setEffect(contractEffect)
        val claimEffect = ClaimEffect()
        claimEffect.contractEffect = contractEffectUnit
        val claim: Claim = buildClaimCreated(claimEffect)
        val partyChange = PartyChange()
        partyChange.claimCreated = claim
        return partyChange
    }

    fun buildShopSuspensionPartyChange(
        shopId: String,
        since: LocalDateTime,
    ): PartyChange {
        val partyChange = PartyChange()
        partyChange.shopSuspension = buildSuspendedShopSuspension(since, shopId)
        return partyChange
    }

    fun buildSuspendedShopSuspension(since: LocalDateTime, shopId: String): ShopSuspension {
        val suspension = Suspension().apply {
            suspended = Suspended(TypeUtil.temporalToString(since))
        }
        return ShopSuspension(shopId, suspension)
    }

    fun buildContractorCreatedPartyChange(partyContractor: PartyContractor): PartyChange {
        val contractorEffectUnit = ContractorEffectUnit()
        contractorEffectUnit.setId(partyContractor.getId())
        val contractorEffect = ContractorEffect()
        contractorEffect.created = partyContractor
        contractorEffectUnit.setEffect(contractorEffect)
        val claimEffect = ClaimEffect()
        claimEffect.contractorEffect = contractorEffectUnit
        val claim: Claim = buildClaimCreated(claimEffect)
        val partyChange = PartyChange()
        partyChange.claimCreated = claim
        return partyChange
    }

    fun buildPartyContractor(): PartyContractor {
        val partyContractor = PartyContractor()
        partyContractor.setId(EnhancedRandom.random(String::class.java))
        partyContractor.setStatus(ContractorIdentificationLevel.full)
        var contractor = Contractor.legal_entity(LegalEntity())
        contractor = MockTBaseProcessor(MockMode.ALL).process(
            contractor,
            TBaseHandler(
                Contractor::class.java
            )
        )
        partyContractor.setContractor(contractor)
        partyContractor.identityDocuments = emptyList()
        return partyContractor
    }

    fun buildContractorIdentificationLevelChangedPartyChange(contractId: String): PartyChange {
        val contractorEffectUnit = ContractorEffectUnit()
        contractorEffectUnit.setId(contractId)
        val contractorEffect = ContractorEffect()
        contractorEffect.identificationLevelChanged = ContractorIdentificationLevel.partial
        contractorEffectUnit.setEffect(contractorEffect)
        val claimEffect = ClaimEffect()
        claimEffect.contractorEffect = contractorEffectUnit
        val claim: Claim = buildClaimCreated(claimEffect)
        val partyChange = PartyChange()
        partyChange.claimCreated = claim
        return partyChange
    }

    fun buildClaimCreated(claimEffect: ClaimEffect): Claim {
        val claimAccepted = ClaimAccepted().apply {
            effects = listOf(claimEffect)
        }
        val claimStatus = ClaimStatus.accepted(claimAccepted)
        return Claim(
            EnhancedRandom.random(Long::class.java),
            claimStatus,
            emptyList(),
            EnhancedRandom.random(Int::class.java),
            TypeUtil.temporalToString(LocalDateTime.now())
        )
    }

    fun buildMachineEvent(sourceId: String, sequenceId: Long, vararg partyChange: PartyChange): MachineEvent {
        val partyChanges = listOf(*partyChange)
        val data = Value().apply {
            bin = PartyEventDataSerializer().serialize(PartyEventData(partyChanges))
        }
        val message = MachineEvent().apply {
            createdAt = TypeUtil.temporalToString(Instant.now())
            eventId = sequenceId
            sourceNs = "sourceNs"
            this.sourceId = sourceId
            this.data = data
        }
        return message
    }

    fun buildSinkEvent(machineEvent: MachineEvent): SinkEvent {
        val sinkEvent = SinkEvent()
        sinkEvent.event = machineEvent
        return sinkEvent
    }
}
