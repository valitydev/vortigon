package com.rbkmoney.porter.listener.handler.merge

interface EventMerger<T> {
    fun mergeEvent(source: T, target: T)
}
