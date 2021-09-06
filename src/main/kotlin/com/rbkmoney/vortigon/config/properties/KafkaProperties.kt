package com.rbkmoney.vortigon.config.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.validation.annotation.Validated
import javax.validation.constraints.NotEmpty
import javax.validation.constraints.NotNull

@Validated
@ConstructorBinding
@ConfigurationProperties(prefix = "kafka")
class KafkaProperties {
    @NotNull
    val ssl: KafkaSslProperties = KafkaSslProperties()
    lateinit var bootstrapServers: String
    lateinit var retryAttempts: String
    lateinit var maxPollRecords: String
    lateinit var maxPollIntervalMs: String
    lateinit var maxSessionTimeoutMs: String
    val consumer = KafkaConsumerProperties()
    var topic = KafkaTopicProperties()

    class KafkaTopicProperties {
        val party = KafkaTopicDetailProperties()
    }

    @Validated
    class KafkaTopicDetailProperties {
        @field:NotEmpty
        lateinit var initial: String

        @field:NotEmpty
        lateinit var maxPollRecords: String
    }

    @Validated
    class KafkaConsumerProperties {
        @field:NotEmpty
        lateinit var prefix: String

        @field:NotEmpty
        lateinit var concurrency: String

        @field:NotEmpty
        lateinit var throttlingTimeoutMs: String
    }

    class KafkaSslProperties {
        var enabled: Boolean = false
        lateinit var keyStoreType: String
        lateinit var keyStoreLocation: String
        lateinit var keyStorePassword: String
        lateinit var keyPassword: String
        lateinit var trustStoreType: String
        lateinit var trustStoreLocation: String
        lateinit var trustStorePassword: String
    }
}
