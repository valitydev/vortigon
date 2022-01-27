package com.rbkmoney.vortigon.config

import dev.vality.damsel.payment_processing.PartyEventData
import dev.vality.sink.common.parser.impl.MachineEventParser
import dev.vality.sink.common.parser.impl.PartyEventDataMachineEventParser
import dev.vality.sink.common.serialization.BinaryDeserializer
import dev.vality.sink.common.serialization.impl.PartyEventDataDeserializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SerializationConfig {
    @Bean
    fun partyEventDataBinaryDeserializer(): BinaryDeserializer<PartyEventData> {
        return PartyEventDataDeserializer()
    }

    @Bean
    fun partyEventDataMachineEventParser(
        partyEventDataBinaryDeserializer: BinaryDeserializer<PartyEventData>,
    ): MachineEventParser<PartyEventData> {
        return PartyEventDataMachineEventParser(partyEventDataBinaryDeserializer)
    }
}
