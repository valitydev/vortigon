package dev.vality.vortigon.handler

import dev.vality.vortigon.handler.constant.HandleEventType
import org.apache.thrift.TBase

interface ChangeHandler<in C : TBase<*, *>, P> {

    fun accept(change: C): Boolean {
        return changeType?.filter?.match(change) ?: false
    }

    fun handleChange(change: C, event: P)

    val changeType: HandleEventType?
        get() = null
}
