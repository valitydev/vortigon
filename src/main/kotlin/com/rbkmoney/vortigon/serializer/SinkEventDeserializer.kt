package com.rbkmoney.vortigon.serializer

import com.rbkmoney.kafka.common.serialization.AbstractThriftDeserializer
import com.rbkmoney.machinegun.eventsink.SinkEvent

class SinkEventDeserializer : AbstractThriftDeserializer<SinkEvent>() {
    override fun deserialize(topic: String, data: ByteArray): SinkEvent {
        return deserialize(data, SinkEvent())
    }
}
