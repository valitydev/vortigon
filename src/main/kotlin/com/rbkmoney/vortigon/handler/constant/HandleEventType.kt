package com.rbkmoney.vortigon.handler.constant

import com.rbkmoney.geck.filter.Condition
import com.rbkmoney.geck.filter.PathConditionFilter
import com.rbkmoney.geck.filter.condition.IsNullCondition
import com.rbkmoney.geck.filter.rule.PathConditionRule

enum class HandleEventType(path: String, vararg conditions: Condition<Any>) {

    PARTY_CREATED("party_created", IsNullCondition().not()),
    PARTY_BLOCKING("party_blocking", IsNullCondition().not()),
    PARTY_SUSPENSION("party_suspension", IsNullCondition().not()),
    CLAIM_CREATED_FILTER("claim_created.status.accepted", IsNullCondition().not()),
    CLAIM_STATUS_CHANGED_FILTER("claim_status_changed.status.accepted", IsNullCondition().not()),
    REVISION_CHANGED("revision_changed", IsNullCondition().not()),
    SHOP_BLOCKING("shop_blocking", IsNullCondition().not()),
    SHOP_SUSPENSION("shop_suspension", IsNullCondition().not());

    val filter: PathConditionFilter = PathConditionFilter(PathConditionRule(path, *conditions))
}
