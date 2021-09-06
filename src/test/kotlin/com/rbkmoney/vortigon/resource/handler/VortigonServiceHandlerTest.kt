package com.rbkmoney.vortigon.resource.handler

import com.rbkmoney.damsel.vortigon.ContractFilter
import com.rbkmoney.damsel.vortigon.PartyFilter
import com.rbkmoney.damsel.vortigon.PartyFilterRequest
import com.rbkmoney.damsel.vortigon.PaymentInstitutionRealm
import com.rbkmoney.damsel.vortigon.ShopFilter
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.vortigon.PostgresAbstractTest
import com.rbkmoney.vortigon.VortigonApplication
import com.rbkmoney.vortigon.domain.db.tables.pojos.Category
import com.rbkmoney.vortigon.domain.db.tables.pojos.Contract
import com.rbkmoney.vortigon.domain.db.tables.pojos.Party
import com.rbkmoney.vortigon.domain.db.tables.pojos.Shop
import com.rbkmoney.vortigon.repository.CategoryDao
import com.rbkmoney.vortigon.repository.ContractDao
import com.rbkmoney.vortigon.repository.PartyDao
import com.rbkmoney.vortigon.repository.ShopDao
import io.github.benas.randombeans.api.EnhancedRandom
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate

@SpringBootTest(classes = [VortigonApplication::class])
class VortigonServiceHandlerTest : PostgresAbstractTest() {

    @Autowired
    lateinit var vortigonServiceHandler: VortigonServiceHandler

    @Autowired
    lateinit var partyDao: PartyDao

    @Autowired
    lateinit var shopDao: ShopDao

    @Autowired
    lateinit var categoryDao: CategoryDao

    @Autowired
    lateinit var contractDao: ContractDao

    @Autowired
    lateinit var postgresJdbcTemplate: JdbcTemplate

    @BeforeEach
    internal fun setUp() {
        postgresJdbcTemplate.execute("TRUNCATE TABLE vrt.category;")
        postgresJdbcTemplate.execute("TRUNCATE TABLE vrt.party;")
        postgresJdbcTemplate.execute("TRUNCATE TABLE vrt.shop;")
        postgresJdbcTemplate.execute("TRUNCATE TABLE vrt.contract;")
    }

    @Test
    fun `test get shop ids`() {
        // Given
        val party = EnhancedRandom.random(Party::class.java)
        val category = EnhancedRandom.random(Category::class.java).apply {
            type = PaymentInstitutionRealm.test.name
        }
        val shop = EnhancedRandom.random(Shop::class.java).apply {
            partyId = party.partyId
            categoryId = category.categoryId
        }

        // When
        partyDao.save(party)
        categoryDao.save(category)
        shopDao.save(shop)
        val shopsIds = vortigonServiceHandler.getShopsIds(party.partyId, PaymentInstitutionRealm.valueOf(category.type))

        // Then
        assertTrue(shopsIds.size == 1)
        assertEquals(shop.shopId, shopsIds.first())
    }

    @Test
    fun `test get party ids by email`() {
        // Given
        val party = EnhancedRandom.random(Party::class.java)
        val partyFilterRequest = PartyFilterRequest().apply {
            partyFilter = PartyFilter().apply {
                contact_info_email = party.email
            }
        }

        // When
        partyDao.save(party)
        val partyIds = vortigonServiceHandler.findPartyIds(partyFilterRequest)

        // Then
        assertTrue(partyIds.size == 1)
        assertEquals(party.partyId, partyIds.first())
    }

    @Test
    fun `test get party ids by location Url`() {
        // Given
        val party = EnhancedRandom.random(Party::class.java)
        val shop = EnhancedRandom.random(Shop::class.java).apply {
            partyId = party.partyId
        }
        val partyFilterRequest = PartyFilterRequest().apply {
            partyFilter = PartyFilter().apply {
                contact_info_email = party.email
            }
            shopFilter = ShopFilter().apply {
                location_url = shop.locationUrl
            }
        }

        // When
        partyDao.save(party)
        shopDao.save(shop)
        val partyIds = vortigonServiceHandler.findPartyIds(partyFilterRequest)

        // Then
        assertTrue(partyIds.size == 1)
        assertEquals(party.partyId, partyIds.first())
    }

    @Test
    fun `test get party ids by contract`() {
        // Given
        val party = EnhancedRandom.random(Party::class.java)
        val contract = EnhancedRandom.random(Contract::class.java).apply {
            partyId = party.partyId
        }
        val partyFilterRequest = PartyFilterRequest().apply {
            partyFilter = PartyFilter().apply {
                contact_info_email = party.email
            }
            contract_filter = ContractFilter().apply {
                legal_agreement_signed_at = TypeUtil.temporalToString(contract.legalAgreementSignedAt)
            }
        }

        // When
        partyDao.save(party)
        contractDao.save(contract)
        val partyIds = vortigonServiceHandler.findPartyIds(partyFilterRequest)

        // Then
        assertTrue(partyIds.size == 1)
        assertEquals(party.partyId, partyIds.first())
    }
}
