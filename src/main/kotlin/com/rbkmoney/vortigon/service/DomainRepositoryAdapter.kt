package com.rbkmoney.vortigon.service

import com.rbkmoney.damsel.domain.Category
import com.rbkmoney.damsel.domain.CategoryRef
import com.rbkmoney.damsel.domain_config.Head
import com.rbkmoney.damsel.domain_config.Reference
import com.rbkmoney.damsel.domain_config.RepositoryClientSrv
import org.springframework.cache.annotation.Cacheable
import org.springframework.stereotype.Component

@Component
class DomainRepositoryAdapter(
    private val repositoryClient: RepositoryClientSrv.Iface,
) {

    @Cacheable("categories")
    fun getCategory(categoryRef: CategoryRef): Category {
        val versionedObject = repositoryClient.checkoutObject(
            Reference.head(Head()),
            com.rbkmoney.damsel.domain.Reference.category(categoryRef)
        )
        if (!versionedObject.isSetObject || !versionedObject.getObject().isSetCategory || !versionedObject.getObject().category.isSetData) {
            throw IllegalArgumentException("Unknown category: ${categoryRef.id}")
        }
        return versionedObject.getObject().category.getData()
    }
}
