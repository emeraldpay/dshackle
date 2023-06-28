package io.emeraldpay.dshackle.proxy

import io.emeraldpay.dshackle.Chain
import io.emeraldpay.dshackle.TlsSetup
import io.emeraldpay.dshackle.config.AuthConfig
import io.emeraldpay.dshackle.config.ProxyConfig
import io.emeraldpay.dshackle.monitoring.accesslog.AccessHandlerHttp
import io.emeraldpay.dshackle.rpc.NativeCall
import io.emeraldpay.dshackle.rpc.NativeSubscribe
import reactor.netty.http.server.HttpServerRoutes
import spock.lang.Specification

class ProxyServerSpec extends Specification {

    def "Setup routes"() {
        setup:
        def config1 = new ProxyConfig()
        config1.routes = [
                new ProxyConfig.Route("test", Chain.ETHEREUM__MAINNET)
        ]
        def proxyServer = new ProxyServer(
                config1,
                new ReadRpcJson(), new WriteRpcJson(),
                Stub(NativeCall), Stub(NativeSubscribe),
                Stub(TlsSetup), new AccessHandlerHttp.NoOpFactory()
        )

        def routes = Mock(HttpServerRoutes)
        when:
        proxyServer.setupRoutes(routes)

        then:
        1 * routes.post("/test", _)
        1 * routes.options("/test", _)
        1 * routes.ws("/test", _)
    }

    def "Setup routes when WS is disabled"() {
        setup:
        def config1 = new ProxyConfig()
        config1.websocketEnabled = false
        config1.routes = [
                new ProxyConfig.Route("test", Chain.ETHEREUM__MAINNET)
        ]
        def proxyServer = new ProxyServer(
                config1,
                new ReadRpcJson(), new WriteRpcJson(),
                Stub(NativeCall), Stub(NativeSubscribe),
                Stub(TlsSetup), new AccessHandlerHttp.NoOpFactory()
        )

        def routes = Mock(HttpServerRoutes)
        when:
        proxyServer.setupRoutes(routes)

        then:
        1 * routes.post("/test", _)
        0 * routes.ws(_, _)
    }

    def "Generate Connect Address"() {
        def config1 = new ProxyConfig()
        config1.host = "192.168.0.1"
        config1.port = 1000
        def proxyServer = new ProxyServer(
                config1,
                new ReadRpcJson(), new WriteRpcJson(),
                Stub(NativeCall), Stub(NativeSubscribe),
                Stub(TlsSetup), new AccessHandlerHttp.NoOpFactory()
        )
        when:
        def act = proxyServer.connectAddress("http")
        then:
        act == "http://192.168.0.1:1000"
    }

    def "Generate Connect Address with TLS"() {
        def config1 = new ProxyConfig()
        config1.host = "192.168.0.1"
        config1.port = 1000
        config1.tls = new AuthConfig.ServerTlsAuth()
        def proxyServer = new ProxyServer(
                config1,
                new ReadRpcJson(), new WriteRpcJson(),
                Stub(NativeCall), Stub(NativeSubscribe),
                Stub(TlsSetup), new AccessHandlerHttp.NoOpFactory()
        )
        when:
        def act = proxyServer.connectAddress("ws")
        then:
        act == "wss://192.168.0.1:1000"
    }
}
