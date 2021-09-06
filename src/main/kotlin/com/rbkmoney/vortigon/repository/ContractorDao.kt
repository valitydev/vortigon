package com.rbkmoney.vortigon.repository

import com.rbkmoney.dao.impl.AbstractGenericDao
import com.rbkmoney.mapper.RecordRowMapper
import com.rbkmoney.vortigon.domain.db.Tables.CONTRACTOR
import com.rbkmoney.vortigon.domain.db.tables.pojos.Contractor
import org.springframework.stereotype.Repository
import javax.sql.DataSource

@Repository
class ContractorDao(postgresDatasource: DataSource) : AbstractGenericDao(postgresDatasource) {

    private val rowMapper = RecordRowMapper(CONTRACTOR, Contractor::class.java)

    fun findByPartyIdAndContractorId(partyId: String, contractorId: String): Contractor? {
        val query = dslContext.selectFrom(CONTRACTOR)
            .where(CONTRACTOR.PARTY_ID.eq(partyId)).and(CONTRACTOR.CONTRACTOR_ID.eq(contractorId))

        return fetchOne(query, rowMapper)
    }

    fun save(contractor: Contractor) {
        val record = dslContext.newRecord(CONTRACTOR, contractor)
        val query = dslContext.insertInto(CONTRACTOR).set(record)
            .onConflict(CONTRACTOR.PARTY_ID, CONTRACTOR.CONTRACTOR_ID)
            .doUpdate()
            .set(record)
        execute(query)
    }
}
