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
package io.emeraldpay.dshackle.test

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec

class IntegrationTestingCommons {

    static boolean isEnabled(String name) {
        return System.getenv("DSHACKLE_TEST_ENABLED") != null && System.getenv("DSHACKLE_TEST_ENABLED").contains(name)
    }

    static boolean isDisabled(String name) {
        return !isEnabled(name)
    }

    static RedisClient redis(int port) {
        String host = "localhost"
        return RedisClient.create("redis://${host}:${port}")
    }

    static StatefulRedisConnection<String, byte[]> redisConnection(int port) {
        return redis(port).connect(RedisCodec.of(StringCodec.ASCII, ByteArrayCodec.INSTANCE))
    }
}
