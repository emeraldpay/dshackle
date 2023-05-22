package io.emeraldpay.dshackle.config.spans

import brave.handler.SpanHandler
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mockito.mock
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.cloud.sleuth.Tracer
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit.jupiter.SpringExtension

class CollectSpanConfigTest {

    @ContextConfiguration(
        classes = [SpanConfig::class],
    )
    @ExtendWith(SpringExtension::class)
    @TestPropertySource(
        properties = [
            "spans.collect.enabled=false"
        ]
    )
    class SpanConfigTest {
        @Autowired
        private lateinit var appCtx: ApplicationContext

        @Test
        fun testSpanConfig() {
            assertThrows(NoSuchBeanDefinitionException::class.java) {
                appCtx.getBean(SpanConfig::class.java)
            }
            assertThrows(NoSuchBeanDefinitionException::class.java) {
                appCtx.getBean(ProviderSpanHandler::class.java)
            }
            assertThrows(NoSuchBeanDefinitionException::class.java) {
                appCtx.getBean(ServerSpansInterceptor::class.java)
            }
            assertThrows(NoSuchBeanDefinitionException::class.java) {
                appCtx.getBean(ClientSpansInterceptor::class.java)
            }
            assertThrows(NoSuchBeanDefinitionException::class.java) {
                appCtx.getBean("spanMapper")
            }
        }
    }

    @ContextConfiguration(
        classes = [SpanConfig::class, SpanProviderConfigTest.Config::class],
    )
    @ExtendWith(SpringExtension::class)
    @TestPropertySource(
        properties = [
            "spans.collect.enabled=true",
            "spans.collect.provider.enabled=true",
            "spans.collect.main.enabled=false"
        ]
    )
    class SpanProviderConfigTest {
        @Autowired
        private lateinit var appCtx: ApplicationContext

        @Test
        fun testSpanProviderConfig() {
            assertDoesNotThrow {
                appCtx.getBean(SpanConfig::class.java)
            }
            assertDoesNotThrow {
                appCtx.getBean(ProviderSpanHandler::class.java)
            }
            assertDoesNotThrow {
                appCtx.getBean(ServerSpansInterceptor::class.java)
            }
            assertThrows(NoSuchBeanDefinitionException::class.java) {
                appCtx.getBean(ClientSpansInterceptor::class.java)
            }
            assertDoesNotThrow {
                appCtx.getBean("spanMapper")
            }
        }

        @TestConfiguration
        open class Config {
            @Bean
            open fun tracer(): Tracer = mock(Tracer::class.java)
        }
    }

    @ContextConfiguration(
        classes = [SpanConfig::class, SpanMainConfigTest.Config::class],
    )
    @ExtendWith(SpringExtension::class)
    @TestPropertySource(
        properties = [
            "spans.collect.enabled=true",
            "spans.collect.provider.enabled=false",
            "spans.collect.main.enabled=true"
        ]
    )
    class SpanMainConfigTest {
        @Autowired
        private lateinit var appCtx: ApplicationContext

        @Test
        fun testSpanMainConfig() {
            assertDoesNotThrow {
                appCtx.getBean(SpanConfig::class.java)
            }
            assertThrows(NoSuchBeanDefinitionException::class.java) {
                appCtx.getBean(ProviderSpanHandler::class.java)
            }
            assertThrows(NoSuchBeanDefinitionException::class.java) {
                appCtx.getBean(ServerSpansInterceptor::class.java)
            }
            assertDoesNotThrow {
                appCtx.getBean(ClientSpansInterceptor::class.java)
            }
            assertDoesNotThrow {
                appCtx.getBean("spanMapper")
            }
        }

        @TestConfiguration
        open class Config {
            @Bean
            open fun tracer(): brave.Tracer = mock(brave.Tracer::class.java)

            @Bean
            open fun zipkinSpanHandler(): SpanHandler = mock(SpanHandler::class.java)
        }
    }
}
