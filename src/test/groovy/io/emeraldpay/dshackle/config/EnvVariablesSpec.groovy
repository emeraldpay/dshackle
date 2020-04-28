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

class EnvVariablesSpec extends Specification {

    EnvVariables reader = new EnvVariables()

    def "Post process for usual strings"() {
        expect:
        s == reader.postProcess(s)
        where:
        s << ["", "a", "13143", "/etc/client1.myservice.com.key", "true", "1a68f20154fc258fe4149c199ad8f281"]
    }

    def "Post process replaces from env"() {
        setup:
        System.setProperty("id", "1")
        System.setProperty("HOME", "/home/user")
        System.setProperty("PASSWORD", "1a68f20154fc258fe4149c199ad8f281")
        expect:
        replaced == reader.postProcess(orig)
        where:
        orig                | replaced
        "p_\${id}"          | "p_1"
        "home: \${HOME}"    | "home: /home/user"
        "\${PASSWORD}"      | "1a68f20154fc258fe4149c199ad8f281"
    }
}
