package com.rbkmoney.vortigon.extension

import dev.vality.damsel.payment_processing.ClaimStatus
import dev.vality.damsel.payment_processing.PartyChange

fun PartyChange.getClaimStatus(): ClaimStatus? {
    return if (this.isSetClaimCreated) {
        this.claimCreated.getStatus()
    } else if (this.isSetClaimStatusChanged) {
        this.claimStatusChanged.getStatus()
    } else null
}
