package com.rbkmoney.vortigon.repository

import com.rbkmoney.dao.impl.AbstractGenericDao
import com.rbkmoney.vortigon.domain.db.Tables.DOMINANT
import com.rbkmoney.vortigon.domain.db.tables.pojos.Dominant
import org.springframework.stereotype.Repository
import javax.sql.DataSource

@Repository
class DominantDao(postgresDatasource: DataSource) : AbstractGenericDao(postgresDatasource) {

    fun updateVersion(version: Long, oldVersion: Long) {
        val query = dslContext
            .update(DOMINANT)
            .set(DOMINANT.LAST_VERSION, version)
            .where(DOMINANT.LAST_VERSION.eq(oldVersion))
        execute(query)
    }

    fun saveVersion(version: Long) {
        val dominantRecord = dslContext.newRecord(DOMINANT, Dominant(version))
        val query = dslContext
            .insertInto(DOMINANT)
            .set(dominantRecord)
        execute(query)
    }

    fun getLastVersion(): Long? {
        val query = dslContext
            .select(DOMINANT.LAST_VERSION)
            .from(DOMINANT)
        return fetchOne(query, Long::class.java)
    }
}
