package io.emeraldpay.dshackle.config

import spock.lang.Specification

class CacheConfigReaderSpec extends Specification {

    CacheConfigReader reader = new CacheConfigReader()

    def "Read full"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("cache-redis-full.yaml")
        when:
        def act = reader.read(config)

        then:
        act.redis != null
        with(act.redis) {
            host == "redis-master"
            port == 1234
            db == 5
            password == "HelloWorld!1"
        }
    }
}
