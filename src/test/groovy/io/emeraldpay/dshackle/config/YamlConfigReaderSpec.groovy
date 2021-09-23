/**
 * Copyright (c) 2021 EmeraldPay, Inc
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

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.nodes.MappingNode
import spock.lang.Specification

class YamlConfigReaderSpec extends Specification {

    def "reads bytes values"() {
        setup:
        def rdr = new Impl()
        expect:
        rdr.getValueAsBytes(asNode("test", input), "test") == exp
        where:
        input  | exp
        "1024" | 1024
        "1k"   | 1024
        "1kb"  | 1024
        "1K"   | 1024
        "16kb" | 16 * 1024
        "1M"   | 1024 * 1024
        "4mb"  | 4 * 1024 * 1024
    }

    private MappingNode asNode(String key, String value) {
        return new Yaml().compose(new StringReader("$key: $value")) as MappingNode
    }

    class Impl extends YamlConfigReader {}
}
