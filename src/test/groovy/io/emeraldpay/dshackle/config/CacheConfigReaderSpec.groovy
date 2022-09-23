/**
 * Copyright (c) 2020 EmeraldPay, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.emeraldpay.dshackle.config

import spock.lang.Specification

class CacheConfigReaderSpec extends Specification {

    CacheConfigReader reader = new CacheConfigReader()

    def "Read full"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/cache-redis-full.yaml")
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

    def "Read disabled"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("configs/cache-redis-disabled.yaml")
        when:
        def act = reader.read(config)

        then:
        //later may be not null if we support something else besides Redis
        act == null
    }

    def "Local read disabled"() {
        setup:
        def config = this.class.getClassLoader().getResourceAsStream("cache-local-router-disabled.yaml")
        when:
        def act = reader.read(config)

        then:
        !act.requestsCacheEnabled
    }
}
