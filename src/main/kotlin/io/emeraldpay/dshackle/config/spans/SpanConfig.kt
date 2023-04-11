package io.emeraldpay.dshackle.config.spans

import brave.Tracer
import brave.handler.SpanHandler
import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import io.grpc.ClientInterceptor
import io.grpc.ServerInterceptor
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

const val SPAN_HEADER = "spans"

@Configuration
@ConditionalOnProperty(value = ["spans.collect.enabled"], havingValue = "true")
open class SpanConfig {

    @Bean
    open fun spanMapper(): ObjectMapper =
        ObjectMapper()
            .apply {
                setSerializationInclusion(JsonInclude.Include.NON_DEFAULT)
                configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                setVisibility(
                    this.serializationConfig.defaultVisibilityChecker
                        .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                        .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                        .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                        .withCreatorVisibility(JsonAutoDetect.Visibility.NONE)
                )
            }

    @Configuration
    @ConditionalOnBean(SpanConfig::class)
    @ConditionalOnProperty(value = ["spans.collect.main.enabled"], havingValue = "true")
    open class MainSpanConfig {
        @Bean
        open fun clientSpansInterceptor(
            zipkinSpanHandler: SpanHandler,
            tracer: Tracer,
            @Qualifier("spanMapper")
            spanMapper: ObjectMapper
        ): ClientInterceptor = ClientSpansInterceptor(zipkinSpanHandler, tracer, spanMapper)
    }

    @Configuration
    @ConditionalOnBean(SpanConfig::class)
    @ConditionalOnProperty(value = ["spans.collect.provider.enabled"], havingValue = "true")
    open class ProviderSpanConfig {

        @Bean
        open fun errorSpanHandler(
            @Qualifier("spanMapper")
            spanMapper: ObjectMapper
        ): ErrorSpanHandler = ErrorSpanHandler(spanMapper)

        @Bean
        open fun serverSpansInterceptor(
            tracer: org.springframework.cloud.sleuth.Tracer,
            errorSpanHandler: ErrorSpanHandler
        ): ServerInterceptor = ServerSpansInterceptor(tracer, errorSpanHandler)
    }
}
