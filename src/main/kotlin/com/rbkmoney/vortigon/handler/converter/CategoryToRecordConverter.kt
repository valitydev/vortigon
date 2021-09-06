package com.rbkmoney.vortigon.handler.converter

import com.rbkmoney.damsel.domain.Category
import com.rbkmoney.damsel.domain.CategoryObject
import org.springframework.stereotype.Component

@Component
class CategoryToRecordConverter :
    DomainConverter<CategoryObject, com.rbkmoney.vortigon.domain.db.tables.pojos.Category> {

    override fun convert(value: CategoryObject): com.rbkmoney.vortigon.domain.db.tables.pojos.Category {
        val category = com.rbkmoney.vortigon.domain.db.tables.pojos.Category()
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
