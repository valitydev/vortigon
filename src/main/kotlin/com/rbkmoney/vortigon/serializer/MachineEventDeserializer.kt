package com.rbkmoney.vortigon.serializer

import com.rbkmoney.machinegun.eventsink.MachineEvent
import com.rbkmoney.machinegun.eventsink.SinkEvent
import mu.KotlinLogging
import org.apache.kafka.common.serialization.Deserializer
import org.apache.thrift.TDeserializer
import org.apache.thrift.protocol.TBinaryProtocol

private val log = KotlinLogging.logger {}

class MachineEventDeserializer : Deserializer<MachineEvent> {

    val thriftDeserializerThreadLocal = ThreadLocal.withInitial {
        TDeserializer(TBinaryProtocol.Factory())
    }

    override fun configure(configs: Map<String, *>, isKey: Boolean) {}

    override fun deserialize(topic: String, data: ByteArray): MachineEvent {
        log.debug("Message, topic=$topic; byteLength=${data.size}", topic, data.size)
        val machineEvent = SinkEvent()
        try {
            thriftDeserializerThreadLocal.get().deserialize(machineEvent, data)
        } catch (e: Exception) {
            log.error(e) { "Error when deserialize data=$data" }
        }
        return machineEvent.event
    }

    override fun close() {}
}
