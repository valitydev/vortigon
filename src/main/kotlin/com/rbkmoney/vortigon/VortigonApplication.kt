package com.rbkmoney.vortigon

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.boot.web.servlet.ServletComponentScan

@ServletComponentScan
@SpringBootApplication
@ConfigurationPropertiesScan
class VortigonApplication : SpringApplication()

fun main(args: Array<String>) {
    runApplication<VortigonApplication>(*args)
}
