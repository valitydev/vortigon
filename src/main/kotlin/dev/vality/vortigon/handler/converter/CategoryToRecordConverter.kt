package dev.vality.vortigon.handler.converter

import dev.vality.damsel.domain.Category
import dev.vality.damsel.domain.CategoryObject
import org.springframework.stereotype.Component

@Component
class CategoryToRecordConverter :
    DomainConverter<CategoryObject, dev.vality.vortigon.domain.db.tables.pojos.Category> {

    override fun convert(value: CategoryObject): dev.vality.vortigon.domain.db.tables.pojos.Category {
        val category = dev.vality.vortigon.domain.db.tables.pojos.Category()
        category.categoryId = value.getRef().getId()
        val data: Category = value.getData()
        category.name = data.getName()
        category.description = data.getDescription()
        if (data.isSetType) {
            category.type = data.getType().name
        }
        return category
    }
}
