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
