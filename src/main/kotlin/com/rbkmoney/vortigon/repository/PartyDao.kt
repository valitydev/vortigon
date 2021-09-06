package com.rbkmoney.vortigon.repository

import com.rbkmoney.dao.impl.AbstractGenericDao
import com.rbkmoney.geck.common.util.TypeUtil
import com.rbkmoney.mapper.RecordRowMapper
import com.rbkmoney.vortigon.domain.db.Tables.CATEGORY
import com.rbkmoney.vortigon.domain.db.Tables.CONTRACT
import com.rbkmoney.vortigon.domain.db.Tables.PARTY
import com.rbkmoney.vortigon.domain.db.Tables.SHOP
import com.rbkmoney.vortigon.domain.db.tables.pojos.Party
import com.rbkmoney.vortigon.repository.model.PartyFilter
import org.jooq.Condition
import org.jooq.Query
import org.jooq.SelectWhereStep
import org.jooq.Table
import org.jooq.impl.DSL
import org.springframework.stereotype.Repository
import javax.sql.DataSource

@Repository
class PartyDao(postgresDatasource: DataSource) : AbstractGenericDao(postgresDatasource) {

    private val rowMapper = RecordRowMapper(PARTY, Party::class.java)

    fun findByPartyId(partyId: String): Party? {
        val query: Query = dslContext.selectFrom(PARTY)
            .where(PARTY.PARTY_ID.eq(partyId))

        return fetchOne(query, rowMapper)
    }

    fun getPartyByFilter(filter: PartyFilter): List<Party> {
        val from: SelectWhereStep<*> = dslContext.selectFrom(buildSelectFrom(filter))
        var condition: Condition = DSL.trueCondition()
        if (filter.email != null) {
            condition = condition.and(PARTY.EMAIL.eq(filter.email))
        }
        if (filter.shopFilter?.locationUrl != null) {
            condition = condition.and(
                SHOP.LOCATION_URL.eq(filter.shopFilter!!.locationUrl)
            )
        }
        if (filter.shopFilter?.categoryName != null) {
            condition = condition.and(
                CATEGORY.NAME.eq(filter.shopFilter!!.categoryName)
            )
        }
        if (filter.contractFilter?.legalAgreementSignedAt != null) {
            condition = condition.and(
                CONTRACT.LEGAL_AGREEMENT_SIGNED_AT.eq(
                    TypeUtil.stringToLocalDateTime(filter.contractFilter!!.legalAgreementSignedAt)
                )
            )
        }
        val query: Query = from.where(condition)
        return fetch(query, rowMapper)
    }

    private fun buildSelectFrom(filter: PartyFilter): Table<*> {
        var from: Table<*> = PARTY
        if (filter.shopFilter != null) {
            from = from.join(SHOP).on(PARTY.PARTY_ID.eq(SHOP.PARTY_ID))
            if (filter.shopFilter!!.categoryName != null) {
                from = from.join(CATEGORY).on(SHOP.CATEGORY_ID.eq(CATEGORY.CATEGORY_ID))
            }
        }
        if (filter.contractFilter != null) {
            from = from.join(CONTRACT).on(PARTY.PARTY_ID.eq(CONTRACT.PARTY_ID))
        }
        return from
    }

    fun save(party: Party) {
        val record = dslContext.newRecord(PARTY, party)
        val query = dslContext.insertInto(PARTY).set(record)
            .onConflict(PARTY.PARTY_ID)
            .doUpdate()
            .set(record)
        execute(query)
    }
}
