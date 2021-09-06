package com.rbkmoney.vortigon.handler.dominant

import com.rbkmoney.damsel.domain.DomainObject
import com.rbkmoney.damsel.domain_config.Operation
import com.rbkmoney.vortigon.domain.db.tables.pojos.Category
import com.rbkmoney.vortigon.extension.domainObject
import com.rbkmoney.vortigon.repository.CategoryDao
import mu.KotlinLogging
import org.springframework.core.convert.ConversionService
import org.springframework.stereotype.Component

private val log = KotlinLogging.logger {}

@Component
class CategoryDominantHandler(
    private val categoryDao: CategoryDao,
    private val conversionService: ConversionService,
) : DominantHandler {

    override fun handle(operation: Operation, versionId: Long) {
        val dominantObject: DomainObject = operation.domainObject()
        val categoryObject = dominantObject.category
        if (operation.isSetInsert) {
            log.info("Save category operation. id=${categoryObject.getRef().getId()}; version=$versionId")
            val category = conversionService.convert(categoryObject, Category::class.java)!!.apply {
                this.versionId = versionId
            }
            categoryDao.save(category)
        } else if (operation.isSetUpdate) {
            log.info("Update category operation. id=${categoryObject.getRef().getId()}; version=$versionId")
            val oldObject = operation.update.oldObject
            val oldCategory = oldObject.category
            val category = conversionService.convert(categoryObject, Category::class.java)!!.apply {
                this.versionId = versionId
            }
            categoryDao.update(oldCategory.getRef().getId(), category)
        } else if (operation.isSetRemove) {
            log.info("Remove category operation. id=${categoryObject.getRef().getId()} version=$versionId")
            val category = conversionService.convert(categoryObject, Category::class.java)!!.apply {
                this.versionId = versionId
            }
            categoryDao.removeCategory(category)
        }
    }

    override fun canHandle(operation: Operation): Boolean {
        val dominantObject = operation.domainObject()
        return dominantObject.isSetCategory
    }
}
