package com.rbkmoney.vortigon.extension

import com.rbkmoney.damsel.payment_processing.ClaimStatus
import com.rbkmoney.damsel.payment_processing.PartyChange

fun PartyChange.getClaimStatus(): ClaimStatus? {
    return if (this.isSetClaimCreated) {
        this.claimCreated.getStatus()
    } else if (this.isSetClaimStatusChanged) {
        this.claimStatusChanged.getStatus()
    } else null
}
