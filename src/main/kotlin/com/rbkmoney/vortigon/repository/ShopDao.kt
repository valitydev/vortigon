package com.rbkmoney.vortigon.repository

import com.rbkmoney.dao.impl.AbstractGenericDao
import com.rbkmoney.mapper.RecordRowMapper
import com.rbkmoney.vortigon.domain.db.Tables.CATEGORY
import com.rbkmoney.vortigon.domain.db.Tables.PARTY
import com.rbkmoney.vortigon.domain.db.Tables.SHOP
import com.rbkmoney.vortigon.domain.db.tables.pojos.Shop
import org.springframework.stereotype.Repository
import javax.sql.DataSource

@Repository
class ShopDao(postgresDatasource: DataSource) : AbstractGenericDao(postgresDatasource) {

    private val rowMapper = RecordRowMapper(SHOP, Shop::class.java)

    fun findByPartyIdAndContractId(partyId: String, contractId: String): MutableList<Shop>? {
        val query = dslContext.selectFrom(SHOP)
            .where(SHOP.PARTY_ID.eq(partyId).and(SHOP.CONTRACT_ID.eq(contractId)))

        return fetch(query, rowMapper)
    }

    fun findByPartyIdAndShopId(partyId: String, shopId: String): Shop? {
        val query = dslContext.selectFrom(SHOP)
            .where(SHOP.PARTY_ID.eq(partyId).and(SHOP.SHOP_ID.eq(shopId)))

        return fetchOne(query, rowMapper)
    }

    fun findShopIdsByPartyIdAndCategoryType(partyId: String, categoryType: String): MutableList<String> {
        val query = dslContext.selectFrom(
            PARTY.join(SHOP).on(PARTY.PARTY_ID.eq(SHOP.PARTY_ID))
                .join(CATEGORY).on(CATEGORY.CATEGORY_ID.eq(SHOP.CATEGORY_ID))
                .where(PARTY.PARTY_ID.eq(partyId).and(CATEGORY.TYPE.eq(categoryType)))
        )

        return fetch(query) { resultSet, _ -> resultSet.getString(SHOP.SHOP_ID.name) } ?: mutableListOf()
    }

    fun saveAll(shopList: List<Shop>) {
        val batchInsertQuery = shopList.map { shop ->
            val record = dslContext.newRecord(SHOP, shop)
            dslContext.insertInto(SHOP).set(record)
                .onConflict(SHOP.PARTY_ID, SHOP.SHOP_ID)
                .doUpdate()
                .set(record)
        }
        batchExecute(batchInsertQuery)
    }

    fun save(shop: Shop) {
        val record = dslContext.newRecord(SHOP, shop)
        val query = dslContext.insertInto(SHOP).set(record)
            .onConflict(SHOP.PARTY_ID, SHOP.SHOP_ID)
            .doUpdate()
            .set(record)
        execute(query)
    }
}
