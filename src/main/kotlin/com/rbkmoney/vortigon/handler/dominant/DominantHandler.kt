package com.rbkmoney.vortigon.handler.dominant

import com.rbkmoney.damsel.domain_config.Operation

interface DominantHandler {
    fun handle(operation: Operation, versionId: Long)
    fun canHandle(operation: Operation): Boolean
}
