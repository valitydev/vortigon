package com.rbkmoney.vortigon.service

import com.rbkmoney.damsel.domain.Category
import com.rbkmoney.damsel.domain.CategoryObject
import com.rbkmoney.damsel.domain.CategoryRef
import com.rbkmoney.damsel.domain.CategoryType
import com.rbkmoney.damsel.domain.DomainObject
import com.rbkmoney.damsel.domain_config.Commit
import com.rbkmoney.damsel.domain_config.InsertOp
import com.rbkmoney.damsel.domain_config.Operation
import com.rbkmoney.damsel.domain_config.RemoveOp
import com.rbkmoney.damsel.domain_config.RepositorySrv
import com.rbkmoney.damsel.domain_config.UpdateOp
import com.rbkmoney.vortigon.PostgresAbstractTest
import com.rbkmoney.vortigon.VortigonApplication
import com.rbkmoney.vortigon.repository.CategoryDao
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.anyInt
import org.mockito.Mockito.anyLong
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.jdbc.core.JdbcTemplate
import java.util.List

@SpringBootTest(classes = [VortigonApplication::class])
class DominantSchedulerTest : PostgresAbstractTest() {

    @Autowired
    lateinit var dominantService: DominantService

    @Autowired
    lateinit var categoryDao: CategoryDao

    @Autowired
    lateinit var postgresJdbcTemplate: JdbcTemplate

    @MockBean
    lateinit var dominantClient: RepositorySrv.Iface

    @BeforeEach
    internal fun setUp() {
        postgresJdbcTemplate.execute("TRUNCATE TABLE vrt.category")
    }

    @Test
    fun `test dominant handle insert commits`() {
        // Given
        val categoryId = 64
        val categoryName = "testName"
        val categoryDescription = "testDescription"
        val categoryType = CategoryType.test
        val version = 1L
        val commitMap =
            mapOf(version to buildInsertCategoryCommit(categoryId, categoryName, categoryDescription, categoryType))

        // When
        whenever(dominantClient.pullRange(anyLong(), anyInt())).thenReturn(commitMap)
        dominantService.pullDominantRange(10)
        val category = categoryDao.getCategory(categoryId, version)

        // Then
        assertEquals(categoryId, category.categoryId)
        assertEquals(categoryName, category.name)
        assertEquals(categoryDescription, category.description)
        assertEquals(categoryType.name, category.type)
    }

    @Test
    fun `test dominant handle update commits`() {
        // Given
        val categoryId = 64
        val categoryName = "testName"
        val categoryDescription = "testDescription"
        val categoryType = CategoryType.test
        val updatedCategoryName = "testNameNew"
        val updatedCategoryDescription = "testDescriptionNew"
        val insertCommit = buildInsertCategoryCommit(categoryId, categoryName, categoryDescription, categoryType)
        val updateCommit = buildUpdateCategoryCommit(
            categoryId,
            updatedCategoryName,
            updatedCategoryDescription,
            categoryType,
            insertCommit.getOps()[0].insert.getObject()
        )
        val commitMap = mapOf(1L to insertCommit, 2L to updateCommit)

        // When
        whenever(dominantClient.pullRange(anyLong(), anyInt())).thenReturn(commitMap)
        dominantService.pullDominantRange(10)
        val category = categoryDao.getCategory(categoryId, 2L)

        // Then
        assertEquals(categoryId, category.categoryId)
        assertEquals(updatedCategoryName, category.name)
        assertEquals(updatedCategoryDescription, category.description)
    }

    @Test
    fun `test dominant handle remove commits`() {
        // Given
        val categoryId = 64
        val categoryName = "testName"
        val categoryDescription = "testDescription"
        val categoryType = CategoryType.test
        val insertCommit = buildInsertCategoryCommit(categoryId, categoryName, categoryDescription, categoryType)
        val removeCommit = buildRemoveCategoryCommit(categoryId, categoryName, categoryDescription, categoryType)
        val commitMap = mapOf(1L to insertCommit, 2L to removeCommit)

        // When
        whenever(dominantClient.pullRange(anyLong(), anyInt())).thenReturn(commitMap)
        dominantService.pullDominantRange(10)
        val category = categoryDao.getCategory(categoryId, 2L)

        // Then
        assertEquals(categoryId, category.categoryId)
        assertEquals(categoryName, category.name)
        assertEquals(categoryDescription, category.description)
        assertEquals(categoryType.name, category.type)
        assertTrue(category.deleted)
    }

    private fun buildInsertCategoryCommit(
        id: Int,
        name: String,
        description: String,
        categoryType: CategoryType
    ): Commit {
        val operation = Operation()
        val insertOp = InsertOp()
        val domainObject = DomainObject()
        domainObject.category = buildCategoryObject(id, name, description, categoryType)
        insertOp.setObject(domainObject)
        operation.insert = insertOp
        return Commit(List.of(operation))
    }

    private fun buildUpdateCategoryCommit(
        id: Int,
        name: String,
        description: String,
        categoryType: CategoryType,
        oldObject: DomainObject
    ): Commit {
        val updateOp = UpdateOp()
        val domainObject = DomainObject()
        domainObject.category = buildCategoryObject(id, name, description, categoryType)
        updateOp.newObject = domainObject
        updateOp.oldObject = oldObject
        val operation = Operation()
        operation.update = updateOp
        return Commit(listOf(operation))
    }

    private fun buildRemoveCategoryCommit(
        id: Int,
        name: String,
        description: String,
        categoryType: CategoryType
    ): Commit {
        val operation = Operation()
        val removeOp = RemoveOp()
        val domainObject = DomainObject()
        domainObject.category = buildCategoryObject(id, name, description, categoryType)
        removeOp.setObject(domainObject)
        operation.remove = removeOp
        return Commit(listOf(operation))
    }

    private fun buildCategoryObject(
        id: Int,
        name: String,
        description: String,
        categoryType: CategoryType
    ): CategoryObject {
        val category = Category()
        category.setName(name)
        category.setDescription(description)
        category.setType(categoryType)
        val categoryObject = CategoryObject()
        categoryObject.setData(category)
        val categoryRef = CategoryRef()
        categoryRef.setId(id)
        categoryObject.setRef(categoryRef)
        return categoryObject
    }
}
