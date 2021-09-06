package com.rbkmoney.vortigon.extension

import com.rbkmoney.damsel.domain.DomainObject
import com.rbkmoney.damsel.domain_config.Operation

fun Operation.domainObject(): DomainObject {
    return if (this.isSetInsert) {
        this.insert.getObject()
    } else if (this.isSetUpdate) {
        this.update.newObject
    } else if (this.isSetRemove) {
        this.remove.getObject()
    } else {
        throw IllegalStateException("Unknown operation type: $this")
    }
}
