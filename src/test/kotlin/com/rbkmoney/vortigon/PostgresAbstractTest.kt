package com.rbkmoney.vortigon

import com.rbkmoney.easyway.EnvironmentProperties
import com.rbkmoney.easyway.TestContainers
import com.rbkmoney.easyway.TestContainersBuilder
import com.rbkmoney.easyway.TestContainersParameters
import mu.KotlinLogging
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.util.TestPropertyValues
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ContextConfiguration
import java.util.function.Consumer

private val log = KotlinLogging.logger {}

@DirtiesContext
@EnableConfigurationProperties(DataSourceProperties::class)
@ContextConfiguration(
    classes = [DataSourceAutoConfiguration::class],
    initializers = [PostgresAbstractTest.Initializer::class]
)
abstract class PostgresAbstractTest {
    class Initializer : ApplicationContextInitializer<ConfigurableApplicationContext> {
        override fun initialize(configurableApplicationContext: ConfigurableApplicationContext) {
            postgresql.startTestContainers()
            TestPropertyValues
                .of(*postgresql.getEnvironmentProperties(environmentPropertiesConsumer))
                .applyTo(configurableApplicationContext)
        }
    }

    companion object {
        private val postgresql: TestContainers =
            TestContainersBuilder.builderWithTestContainers { TestContainersParameters() }
                .addPostgresqlTestContainer()
                .build()
        private val environmentPropertiesConsumer: Consumer<EnvironmentProperties>
            get() = Consumer { environmentProperties: EnvironmentProperties ->
                val postgreSQLContainer = postgresql.postgresqlTestContainer.get()
                environmentProperties.put("spring.datasource.url", postgreSQLContainer.jdbcUrl)
                environmentProperties.put("spring.datasource.username", postgreSQLContainer.username)
                environmentProperties.put("spring.datasource.password", postgreSQLContainer.password)
                environmentProperties.put("spring.flyway.url", postgreSQLContainer.jdbcUrl)
                environmentProperties.put("spring.flyway.user", postgreSQLContainer.username)
                environmentProperties.put("spring.flyway.password", postgreSQLContainer.password)
            }
    }
}
