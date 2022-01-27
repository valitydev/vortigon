package dev.vality.vortigon.handler.constant

import dev.vality.geck.filter.Condition
import dev.vality.geck.filter.PathConditionFilter
import dev.vality.geck.filter.condition.IsNullCondition
import dev.vality.geck.filter.rule.PathConditionRule

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
