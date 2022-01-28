package dev.vality.vortigon.repository

import dev.vality.dao.impl.AbstractGenericDao
import dev.vality.mapper.RecordRowMapper
import dev.vality.vortigon.domain.db.Tables.CONTRACT
import dev.vality.vortigon.domain.db.tables.pojos.Contract
import org.jooq.Query
import org.springframework.stereotype.Repository
import javax.sql.DataSource

@Repository
class ContractDao(postgresDatasource: DataSource) : AbstractGenericDao(postgresDatasource) {

    private val rowMapper = RecordRowMapper(CONTRACT, Contract::class.java)

    fun findByPartyIdAndContractId(partyId: String, contractId: String): Contract? {
        val query: Query = dslContext.selectFrom(CONTRACT)
            .where(CONTRACT.PARTY_ID.eq(partyId).and(CONTRACT.CONTRACT_ID.eq(contractId)))

        return fetchOne(query, rowMapper)
    }

    fun save(contract: Contract) {
        val record = dslContext.newRecord(CONTRACT, contract)
        val query = dslContext.insertInto(CONTRACT).set(record)
            .onConflict(CONTRACT.PARTY_ID, CONTRACT.CONTRACT_ID)
            .doUpdate()
            .set(record)
        execute(query)
    }
}
