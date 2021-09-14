package com.rbkmoney.vortigon.listener

import com.rbkmoney.damsel.domain.Category
import com.rbkmoney.damsel.domain.CategoryType
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.machinegun.eventsink.SinkEvent
import com.rbkmoney.vortigon.AbstractKafkaIntegrationTest
import com.rbkmoney.vortigon.VortigonApplication
import com.rbkmoney.vortigon.domain.db.enums.ContractStatus
import com.rbkmoney.vortigon.domain.db.enums.Suspension
import com.rbkmoney.vortigon.repository.ContractDao
import com.rbkmoney.vortigon.repository.ContractorDao
import com.rbkmoney.vortigon.repository.PartyDao
import com.rbkmoney.vortigon.repository.ShopDao
import com.rbkmoney.vortigon.service.DomainRepositoryAdapter
import io.github.benas.randombeans.api.EnhancedRandom
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.awaitility.Awaitility.await
import org.awaitility.Durations
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.jdbc.core.JdbcTemplate
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

@SpringBootTest(classes = [VortigonApplication::class])
class PartyManagementListenerTest : AbstractKafkaIntegrationTest() {

    @Autowired
    lateinit var partyShopDao: ShopDao

    @Autowired
    lateinit var partyDao: PartyDao

    @Autowired
    lateinit var shopDao: ShopDao

    @Autowired
    lateinit var contractDao: ContractDao

    @Autowired
    lateinit var contractorDao: ContractorDao

    @Autowired
    lateinit var postgresJdbcTemplate: JdbcTemplate

    @MockBean
    lateinit var domainRepositoryAdapter: DomainRepositoryAdapter

    @Value("\${kafka.topic.party.initial}")
    lateinit var partyTopic: String

    private val producer: Producer<String, SinkEvent> by lazy {
        return@lazy createProducer()
    }

    @BeforeEach
    internal fun setUp() {
        whenever(domainRepositoryAdapter.getCategory(anyOrNull())).then {
            Category().apply {
                name = "testCategoryName"
                description = "testDescription"
                type = CategoryType.test
            }
        }
        postgresJdbcTemplate.execute("TRUNCATE TABLE vrt.party;")
        postgresJdbcTemplate.execute("TRUNCATE TABLE vrt.shop;")
        postgresJdbcTemplate.execute("TRUNCATE TABLE vrt.contract;")
        postgresJdbcTemplate.execute("TRUNCATE TABLE vrt.contractor;")
    }

    @Test
    fun `test party create event handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val events = mutableListOf<SinkEvent>().apply {
            val partyChange = PartyEventBuilder.buildPartyCreatedPartyChange(partyId)
            val machineEvent = PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyChange)
            add(PartyEventBuilder.buildSinkEvent(machineEvent))
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val party = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                partyDao.findByPartyId(partyId)
            },
            { it != null }
        )

        // Then
        assertNotNull(party)
        assertEquals(partyId, party?.partyId)
    }

    @Test
    fun `test party blocking event handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val partyCreateChange = PartyEventBuilder.buildPartyCreatedPartyChange(partyId)
        val blockPartyChange = PartyEventBuilder.buildPartyBlockingPartyChange()
        val events = mutableListOf<SinkEvent>().apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyCreateChange)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), blockPartyChange)
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val party = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                partyDao.findByPartyId(partyId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(party)
        assertEquals(partyId, party?.partyId)
        assertEquals(sequenceId.get(), party?.eventId)
        assertEquals(blockPartyChange.partyBlocking.blocked.reason, party?.blockedReason)
        assertEquals(
            TypeUtil.stringToLocalDateTime(blockPartyChange.partyBlocking.blocked.since).withNano(0),
            party?.blockedSince?.withNano(0)
        )
    }

    @Test
    fun `test party revision event handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val partyCreateChange = PartyEventBuilder.buildPartyCreatedPartyChange(partyId)
        val revisionPartyChange = PartyEventBuilder.buildPartyRevisionChangedPartyChange()
        val events = mutableListOf<SinkEvent>().apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyCreateChange)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), revisionPartyChange)
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val party = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                partyDao.findByPartyId(partyId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(party)
        assertEquals(partyId, party?.partyId)
        assertEquals(sequenceId.get(), party?.eventId)
        assertEquals(revisionPartyChange.revisionChanged.revision, party?.revisionId?.toLong())
        assertEquals(
            TypeUtil.stringToLocalDateTime(revisionPartyChange.revisionChanged.timestamp).withNano(0),
            party?.revisionChangedAt?.withNano(0)
        )
    }

    @Test
    fun `test party suspension event handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val partyCreateChange = PartyEventBuilder.buildPartyCreatedPartyChange(partyId)
        val suspensionPartyChange = PartyEventBuilder.buildPartySuspensionPartyChange()
        val events = mutableListOf<SinkEvent>().apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyCreateChange)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), suspensionPartyChange)
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val party = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                partyDao.findByPartyId(partyId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(party)
        assertEquals(partyId, party?.partyId)
        assertEquals(sequenceId.get(), party?.eventId)
        assertTrue(party?.suspension == Suspension.active)
        assertEquals(
            TypeUtil.stringToLocalDateTime(suspensionPartyChange.partySuspension.active.since).withNano(0),
            party?.suspensionActiveSince?.withNano(0)
        )
    }

    @Test
    fun `test contract create event handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val contractId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val partyCreatedPartyChange = PartyEventBuilder.buildPartyCreatedPartyChange(partyId)
        val contractCreatedPartyChange =
            PartyEventBuilder.buildContractCreatedPartyChange(PartyEventBuilder.buildContract(contractId))
        val events = mutableListOf<SinkEvent>().apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyCreatedPartyChange)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(
                        partyId,
                        sequenceId.incrementAndGet(),
                        contractCreatedPartyChange
                    )
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val contract = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                contractDao.findByPartyIdAndContractId(partyId, contractId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(contract)
        assertEquals(partyId, contract?.partyId)
        assertEquals(contractId, contract?.contractId)
        assertEquals(sequenceId.get(), contract?.eventId)
        assertTrue(contract?.status == ContractStatus.active)
    }

    @Test
    fun `test contract contractorId change event handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val contractId = UUID.randomUUID().toString()
        val contractorId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val partyCreatedPartyChange = PartyEventBuilder.buildPartyCreatedPartyChange(partyId)
        val newContract = PartyEventBuilder.buildContract(contractId)
        val contractCreatedPartyChange = PartyEventBuilder.buildContractCreatedPartyChange(newContract)
        val contractContractorIdChange = PartyEventBuilder.buildContractContractorIdChange(newContract, contractorId)
        val events = mutableListOf<SinkEvent>().apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyCreatedPartyChange)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), contractCreatedPartyChange)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), contractContractorIdChange)
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val contract = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                contractDao.findByPartyIdAndContractId(partyId, contractId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(contract)
        assertEquals(contractorId, contract?.contractorId)
    }

    @Test
    fun `test contract contractorId change with shops event handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val contractId = UUID.randomUUID().toString()
        val shopId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val partyCreatedPartyChange = PartyEventBuilder.buildPartyCreatedPartyChange(partyId)
        val newContract = PartyEventBuilder.buildContract(contractId) // TODO пофиксить разный contractorId
        val contractCreatedPartyChange = PartyEventBuilder.buildContractCreatedPartyChange(newContract)
        val partyContractor = PartyEventBuilder.buildPartyContractor().apply {
            id = contractId
        }
        val partyContractorChange = PartyEventBuilder.buildContractorCreatedPartyChange(partyContractor)
        val contractContractorIdChange = PartyEventBuilder.buildContractContractorIdChange(newContract, partyContractor.id)
        val shopCreated = PartyEventBuilder.buildShopCreatedPartyChange(shopId)
        shopCreated.claimCreated.status.accepted.effects
            .first { it.isSetShopEffect }.shopEffect.effect.created.contractId = contractId
        val events = mutableListOf<SinkEvent>().apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyCreatedPartyChange)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyContractorChange)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), contractCreatedPartyChange)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), shopCreated)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), contractContractorIdChange)
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val contract = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                contractDao.findByPartyIdAndContractId(partyId, contractId)
            },
            { it != null && it.eventId == sequenceId.get() && it.contractorId == partyContractor.id }
        )
        val shop = shopDao.findByPartyIdAndShopId(partyId, shopId)

        // Then
        assertNotNull(contract)
        assertEquals(partyContractor.id, contract?.contractorId)
        assertTrue(shop?.eventId == sequenceId.get())
    }

    @Test
    fun `test shop create event handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val contractId = UUID.randomUUID().toString()
        val shopId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val shopCreated = PartyEventBuilder.buildShopCreatedPartyChange(shopId)
        shopCreated.claimCreated.status.accepted.effects
            .first { it.isSetShopEffect }.shopEffect.effect.created.contractId = contractId
        val contractCreatedPartyChange =
            PartyEventBuilder.buildContractCreatedPartyChange(PartyEventBuilder.buildContract(contractId))
        val events = mutableListOf<SinkEvent>().apply {
            val partyChange = PartyEventBuilder.buildPartyCreatedPartyChange(partyId)
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyChange)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(
                        partyId,
                        sequenceId.incrementAndGet(),
                        contractCreatedPartyChange
                    )
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), shopCreated)
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val shop = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                shopDao.findByPartyIdAndShopId(partyId, shopId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )
        val partyShop = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                partyShopDao.findByPartyIdAndShopId(partyId, shopId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(shop)
        assertEquals(partyId, shop?.partyId)
        assertEquals(shopId, shop?.shopId)
        assertEquals(sequenceId.get(), shop?.eventId)
        assertEquals(partyId, partyShop?.partyId)
        assertEquals(shopId, partyShop?.shopId)
        assertEquals(sequenceId.get(), partyShop?.eventId)
    }

    @Test
    fun `test shop account event handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val shopId = UUID.randomUUID().toString()
        val contractId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val shopAccountCreatedPartyChange = PartyEventBuilder.buildShopAccountCreatedPartyChange(shopId)
        val events = buildCreateShopEvents(partyId, shopId, contractId, sequenceId).apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(
                        partyId,
                        sequenceId.incrementAndGet(),
                        shopAccountCreatedPartyChange
                    )
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val shop = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                shopDao.findByPartyIdAndShopId(partyId, shopId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(shop)
        assertEquals(partyId, shop?.partyId)
        assertEquals(shopId, shop?.shopId)
        assertEquals(sequenceId.get(), shop?.eventId)
        val accountCreated = shopAccountCreatedPartyChange.claimCreated.status.accepted.effects
            .first { it.isSetShopEffect }.shopEffect.effect.accountCreated
        assertEquals(accountCreated.getCurrency().symbolicCode, shop?.accountCurrencyCode)
        assertEquals(accountCreated.guarantee, shop?.accountGuarantee?.toLong())
        assertEquals(accountCreated.settlement, shop?.accountSettlement?.toLong())
        assertEquals(accountCreated.payout, shop?.accountPayout?.toLong())
    }

    @Test
    fun `test shop blocking event handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val shopId = UUID.randomUUID().toString()
        val contractId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val shopBlockingPartyChange = PartyEventBuilder.buildShopBlockingPartyChange(shopId)
        val events = buildCreateShopEvents(partyId, shopId, contractId, sequenceId).apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(
                        partyId,
                        sequenceId.incrementAndGet(),
                        shopBlockingPartyChange
                    )
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val shop = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                shopDao.findByPartyIdAndShopId(partyId, shopId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(shop)
        assertEquals(partyId, shop?.partyId)
        assertEquals(shopId, shop?.shopId)
        assertEquals(sequenceId.get(), shop?.eventId)
        assertEquals(
            TypeUtil.stringToLocalDateTime(shopBlockingPartyChange.shopBlocking.blocking.unblocked.since).withNano(0),
            shop?.unblockedSince?.withNano(0)
        )
        assertEquals(
            shopBlockingPartyChange.shopBlocking.blocking.unblocked.reason,
            shop?.unblockedReason
        )
    }

    @Test
    fun `test shop category event handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val shopId = UUID.randomUUID().toString()
        val categoryId = EnhancedRandom.random(Int::class.java)
        val contractId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val shopCategoryPartyChange = PartyEventBuilder.buildShopCategoryPartyChange(shopId, categoryId)
        val events = buildCreateShopEvents(partyId, shopId, contractId, sequenceId).apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(
                        partyId,
                        sequenceId.incrementAndGet(),
                        shopCategoryPartyChange
                    )
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val shop = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                shopDao.findByPartyIdAndShopId(partyId, shopId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(shop)
        assertEquals(partyId, shop?.partyId)
        assertEquals(shopId, shop?.shopId)
        assertEquals(sequenceId.get(), shop?.eventId)
        assertEquals(categoryId, shop?.categoryId)
    }

    @Test
    fun `test shop details event handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val shopId = UUID.randomUUID().toString()
        val contractId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val shopCategoryPartyChange = PartyEventBuilder.buildShopDetailsPartyChange(shopId)
        val events = buildCreateShopEvents(partyId, shopId, contractId, sequenceId).apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(
                        partyId,
                        sequenceId.incrementAndGet(),
                        shopCategoryPartyChange
                    )
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val shop = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                shopDao.findByPartyIdAndShopId(partyId, shopId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(shop)
        assertEquals(partyId, shop?.partyId)
        assertEquals(shopId, shop?.shopId)
        assertEquals(sequenceId.get(), shop?.eventId)
        val claimEffect =
            shopCategoryPartyChange.claimCreated.status.accepted.effects
                .find { it.isSetShopEffect && it.shopEffect.effect.isSetDetailsChanged }
        val detailsChanged = claimEffect?.shopEffect?.effect?.detailsChanged
        assertEquals(detailsChanged?.name, shop?.detailsName)
        assertEquals(detailsChanged?.description, shop?.detailsDescription)
    }

    @Test
    fun `test shop location event handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val shopId = UUID.randomUUID().toString()
        val contractId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val shopLocationPartyChange = PartyEventBuilder.buildShopLocationPartyChange(shopId)
        val events = buildCreateShopEvents(partyId, shopId, contractId, sequenceId).apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(
                        partyId,
                        sequenceId.incrementAndGet(),
                        shopLocationPartyChange
                    )
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val shop = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                shopDao.findByPartyIdAndShopId(partyId, shopId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(shop)
        assertEquals(partyId, shop?.partyId)
        assertEquals(shopId, shop?.shopId)
        assertEquals(sequenceId.get(), shop?.eventId)
        val claimEffect =
            shopLocationPartyChange.claimCreated.status.accepted.effects
                .find { it.isSetShopEffect && it.shopEffect.effect.isSetLocationChanged }
        val locationChanged = claimEffect?.shopEffect?.effect?.locationChanged
        assertEquals(locationChanged?.url, shop?.locationUrl)
    }

    @Test
    fun `test shop payout schedule handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val shopId = UUID.randomUUID().toString()
        val contractId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val shopPayoutSchedulerChange = PartyEventBuilder.buildShopPayoutScheduleChangedPartyChange(shopId)
        val events = buildCreateShopEvents(partyId, shopId, contractId, sequenceId).apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(
                        partyId,
                        sequenceId.incrementAndGet(),
                        shopPayoutSchedulerChange
                    )
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val shop = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                shopDao.findByPartyIdAndShopId(partyId, shopId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(shop)
        assertEquals(partyId, shop?.partyId)
        assertEquals(shopId, shop?.shopId)
        assertEquals(sequenceId.get(), shop?.eventId)
        val claimEffect =
            shopPayoutSchedulerChange.claimCreated.status.accepted.effects
                .find { it.isSetShopEffect && it.shopEffect.effect.isSetPayoutScheduleChanged }
        val payoutScheduleChange = claimEffect?.shopEffect?.effect?.payoutScheduleChanged
        assertEquals(payoutScheduleChange?.schedule?.id, shop?.payoutScheduleId)
    }

    @Test
    fun `test shop payout tool handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val shopId = UUID.randomUUID().toString()
        val contractId = UUID.randomUUID().toString()
        val payoutToolId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()

        val shopPayoutToolPartyChange = PartyEventBuilder.buildShopPayoutToolChangedPartyChange(shopId, payoutToolId)
        val events = buildCreateShopEvents(partyId, shopId, contractId, sequenceId).apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(
                        partyId,
                        sequenceId.incrementAndGet(),
                        shopPayoutToolPartyChange
                    )
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val shop = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                shopDao.findByPartyIdAndShopId(partyId, shopId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(shop)
        assertEquals(partyId, shop?.partyId)
        assertEquals(shopId, shop?.shopId)
        assertEquals(sequenceId.get(), shop?.eventId)
        assertEquals(payoutToolId, shop?.payoutToolId)
    }

    @Test
    fun `test shop suspension handle`() {
        val partyId = UUID.randomUUID().toString()
        val shopId = UUID.randomUUID().toString()
        val contractId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()

        val shopSuspension = PartyEventBuilder.buildShopSuspensionPartyChange(shopId, LocalDateTime.now())
        val events = buildCreateShopEvents(partyId, shopId, contractId, sequenceId).apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(
                        partyId,
                        sequenceId.incrementAndGet(),
                        shopSuspension
                    )
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val shop = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                shopDao.findByPartyIdAndShopId(partyId, shopId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(shop)
        assertEquals(partyId, shop?.partyId)
        assertEquals(shopId, shop?.shopId)
        assertEquals(sequenceId.get(), shop?.eventId)
        assertTrue(shop?.suspension == Suspension.suspended)
        assertEquals(
            TypeUtil.stringToLocalDateTime(shopSuspension.shopSuspension.suspension.suspended.since).withNano(0),
            shop?.suspensionSuspendedSince?.withNano(0)
        )
    }

    @Test
    fun `test contractor create`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val contractId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val partyCreateChange = PartyEventBuilder.buildPartyCreatedPartyChange(partyId)
        val partyContractor = PartyEventBuilder.buildPartyContractor().apply {
            id = contractId
        }
        val partyContractorChange = PartyEventBuilder.buildContractorCreatedPartyChange(partyContractor)
        val events = mutableListOf<SinkEvent>().apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyCreateChange)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyContractorChange)
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val contractor = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                contractorDao.findByPartyIdAndContractorId(partyId, contractId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(contractor)
        assertEquals(partyId, contractor?.partyId)
        assertEquals(contractId, contractor?.contractorId)
        assertEquals(sequenceId.get(), contractor?.eventId)
    }

    @Test
    fun `test contractor identificationLevel handle`() {
        // Given
        val partyId = UUID.randomUUID().toString()
        val contractId = UUID.randomUUID().toString()
        val sequenceId = AtomicLong()
        val partyCreateChange = PartyEventBuilder.buildPartyCreatedPartyChange(partyId)
        val partyContractorChange = PartyEventBuilder.buildContractorIdentificationLevelChangedPartyChange(contractId)
        val events = mutableListOf<SinkEvent>().apply {
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyCreateChange)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyContractorChange)
                )
            )
        }

        // When
        events.forEach {
            val record = ProducerRecord<String, SinkEvent>(partyTopic, it.event.sourceId, it)
            producer.send(record)
        }
        val contractor = await().atMost(60, TimeUnit.SECONDS).pollDelay(Durations.ONE_SECOND).until(
            {
                contractorDao.findByPartyIdAndContractorId(partyId, contractId)
            },
            { it != null && it.eventId == sequenceId.get() }
        )

        // Then
        assertNotNull(contractor)
        assertEquals(partyId, contractor?.partyId)
        assertEquals(contractId, contractor?.contractorId)
        assertEquals(sequenceId.get(), contractor?.eventId)
        val claimEffect = partyContractorChange.claimCreated.status.accepted.effects
            .find { it.isSetContractorEffect && it.contractorEffect.effect.isSetIdentificationLevelChanged }
        assertEquals(
            claimEffect!!.contractorEffect.effect.identificationLevelChanged.name,
            contractor?.contractorIdentificationLevel?.name
        )
    }

    private fun buildCreateShopEvents(
        partyId: String,
        shopId: String,
        contractId: String,
        sequenceId: AtomicLong
    ): MutableList<SinkEvent> {
        return mutableListOf<SinkEvent>().apply {
            val partyChange = PartyEventBuilder.buildPartyCreatedPartyChange(partyId)
            val shopCreated = PartyEventBuilder.buildShopCreatedPartyChange(shopId)
            shopCreated.claimCreated.status.accepted.effects
                .first { it.isSetShopEffect }.shopEffect.effect.created.contractId = contractId
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), partyChange)
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(
                        partyId,
                        sequenceId.incrementAndGet(),
                        PartyEventBuilder.buildContractCreatedPartyChange(PartyEventBuilder.buildContract(contractId))
                    )
                )
            )
            add(
                PartyEventBuilder.buildSinkEvent(
                    PartyEventBuilder.buildMachineEvent(partyId, sequenceId.incrementAndGet(), shopCreated)
                )
            )
        }
    }
}
