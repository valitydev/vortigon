package com.rbkmoney.vortigon.scheduler

import com.rbkmoney.vortigon.service.DominantService
import mu.KotlinLogging
import net.javacrumbs.shedlock.core.LockAssert
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

private val log = KotlinLogging.logger {}

@Service
@ConditionalOnProperty(prefix = "service", name = ["dominant.scheduler.enabled"], havingValue = "true")
class DominantScheduler(
    private val dominantService: DominantService,
    @Value("\${service.dominant.scheduler.querySize:0}") private val querySize: Int
) {

    @Scheduled(fixedDelayString = "\${service.dominant.scheduler.pollingDelay}")
    @SchedulerLock(name = "dominantPullTask")
    fun pollScheduler() {
        try {
            LockAssert.assertLocked()
            dominantService.pullDominantRange(querySize)
        } catch (e: Exception) {
            log.error(e) { "Dominant pullRange failed" }
        }
    }
}
