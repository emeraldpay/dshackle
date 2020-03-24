package io.emeraldpay.dshackle.config

class MainConfig {
    var host = "127.0.0.1"
    var port = 2449
    var tls: AuthConfig.ServerTlsAuth? = null
    var cache: CacheConfig? = null
    var proxy: ProxyConfig? = null
    var upstreams: UpstreamsConfig? = null

}