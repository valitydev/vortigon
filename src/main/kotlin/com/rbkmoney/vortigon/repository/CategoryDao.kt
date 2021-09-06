package com.rbkmoney.vortigon.repository

import com.rbkmoney.dao.impl.AbstractGenericDao
import com.rbkmoney.mapper.RecordRowMapper
import com.rbkmoney.vortigon.domain.db.Tables.CATEGORY
import com.rbkmoney.vortigon.domain.db.tables.pojos.Category
import org.jooq.Query
import org.springframework.stereotype.Repository
import javax.sql.DataSource

@Repository
class CategoryDao(postgresDatasource: DataSource) : AbstractGenericDao(postgresDatasource) {

    private val rowMapper = RecordRowMapper(CATEGORY, Category::class.java)

    fun save(category: Category) {
        val record = dslContext.newRecord(CATEGORY, category)
        val query = dslContext.insertInto(CATEGORY).set(record)
        execute(query)
    }

    fun update(categoryId: Int, category: Category) {
        val query = dslContext.update(CATEGORY)
            .set(CATEGORY.VERSION_ID, category.versionId)
            .set(CATEGORY.NAME, category.name)
            .set(CATEGORY.DESCRIPTION, category.description)
            .set(CATEGORY.TYPE, category.type)
            .where(CATEGORY.CATEGORY_ID.eq(categoryId))
        execute(query)
    }

    fun removeCategory(category: Category) {
        val query = dslContext.update(CATEGORY)
            .set(CATEGORY.DELETED, true)
            .set(CATEGORY.VERSION_ID, category.versionId)
            .where(CATEGORY.CATEGORY_ID.eq(category.categoryId))
        execute(query)
    }

    fun getCategory(categoryId: Int, versionId: Long): Category {
        val query: Query = dslContext.selectFrom(CATEGORY)
            .where(CATEGORY.CATEGORY_ID.eq(categoryId))
            .and(CATEGORY.VERSION_ID.eq(versionId))
        return fetchOne(query, rowMapper)
    }
}
