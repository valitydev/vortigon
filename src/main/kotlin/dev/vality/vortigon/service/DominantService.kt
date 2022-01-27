package dev.vality.vortigon.service

import dev.vality.vortigon.extension.toJson
import dev.vality.vortigon.handler.dominant.DominantHandler
import dev.vality.vortigon.repository.DominantDao
import dev.vality.damsel.domain_config.Operation
import dev.vality.damsel.domain_config.RepositorySrv
import mu.KotlinLogging
import org.springframework.stereotype.Service
import java.util.Map
import java.util.concurrent.atomic.AtomicLong
import javax.transaction.Transactional

private val log = KotlinLogging.logger {}

@Service
class DominantService(
    private val dominantClient: RepositorySrv.Iface,
    private val dominantDao: DominantDao,
    private val dominantHandlers: List<DominantHandler>,
) {

    @Transactional
    fun pullDominantRange(querySize: Int) {
        val initialVersion = dominantDao.getLastVersion() ?: 0L
        val lastVer = AtomicLong(initialVersion)
        val commitMap = dominantClient.pullRange(lastVer.get(), querySize)
        commitMap.entries.stream().sorted(Map.Entry.comparingByKey()).forEach { entry ->
            val operations: List<Operation> = entry.value.getOps()
            operations.forEach { operation ->
                dominantHandlers.forEach { handler ->
                    if (handler.canHandle(operation)) {
                        log.info("Process commit with versionId={} operation={} ", entry.key, operation.toJson())
                        handler.handle(operation, entry.key)
                    }
                }
            }
            if (lastVer.get() == 0L) {
                log.info("Save dominant version: ${entry.key}")
                dominantDao.saveVersion(entry.key)
            } else {
                log.info("Update dominant version=${entry.key} oldVersion=${lastVer.get()}")
                dominantDao.updateVersion(entry.key, lastVer.get())
            }
            lastVer.set(entry.key)
        }
    }
}
