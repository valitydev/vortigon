package com.rbkmoney.vortigon.handler.converter

import org.springframework.core.convert.converter.Converter

interface DomainConverter<S, T> : Converter<S, T>
