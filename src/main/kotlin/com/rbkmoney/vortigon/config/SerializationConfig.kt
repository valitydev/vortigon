package com.rbkmoney.vortigon.config

import com.rbkmoney.damsel.payment_processing.PartyEventData
import com.rbkmoney.sink.common.parser.impl.MachineEventParser
import com.rbkmoney.sink.common.parser.impl.PartyEventDataMachineEventParser
import com.rbkmoney.sink.common.serialization.BinaryDeserializer
import com.rbkmoney.sink.common.serialization.impl.PartyEventDataDeserializer
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
