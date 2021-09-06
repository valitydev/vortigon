package com.rbkmoney.vortigon.config

import net.javacrumbs.shedlock.core.LockProvider
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.EnableScheduling

@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "\${scheduler.lockFor}")
class SchedulerConfig {

    @Bean
    fun lockProvider(postgresJdbcTemplate: JdbcTemplate): LockProvider {
        return JdbcTemplateLockProvider(
            JdbcTemplateLockProvider.Configuration.builder()
                .withJdbcTemplate(postgresJdbcTemplate)
                .withTableName("vrt.shedlock")
                .usingDbTime()
                .build()
        )
    }
}
