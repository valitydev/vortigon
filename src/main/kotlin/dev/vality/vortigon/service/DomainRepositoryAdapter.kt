package dev.vality.vortigon.service

import dev.vality.damsel.domain.Category
import dev.vality.damsel.domain.CategoryRef
import dev.vality.damsel.domain_config.Head
import dev.vality.damsel.domain_config.Reference
import dev.vality.damsel.domain_config.RepositoryClientSrv
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
            dev.vality.damsel.domain.Reference.category(categoryRef)
        )
        if (!versionedObject.isSetObject || !versionedObject.getObject().isSetCategory || !versionedObject.getObject().category.isSetData) {
            throw IllegalArgumentException("Unknown category: ${categoryRef.id}")
        }
        return versionedObject.getObject().category.getData()
    }
}
