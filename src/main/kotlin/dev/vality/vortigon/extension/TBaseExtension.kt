package dev.vality.vortigon.extension

import dev.vality.geck.serializer.kit.json.JsonHandler
import dev.vality.geck.serializer.kit.tbase.TBaseProcessor
import org.apache.thrift.TBase
import org.apache.thrift.TFieldIdEnum

fun <T : TBase<T, F>, F : TFieldIdEnum> TBase<T, F>.toJson(): String {
    return TBaseProcessor().process(this, JsonHandler()).toString()
}
