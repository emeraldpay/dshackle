package io.emeraldpay.dshackle.config

import spock.lang.Specification

class AuthConfigReaderSpec extends Specification {

    AuthConfigReader reader = new AuthConfigReader()

    def "Read basic-auth for client"() {
        setup:
        def yaml =
                "basic-auth:\n" +
                        "  username: 9c199ad8f281f20154fc258fe41a6814\n" +
                        "  password: 258fe4149c199ad8f2811a68f20154fc"
        when:
        def act = reader.readClientBasicAuth(reader.readNode(yaml))
        then:
        act != null
        act.username == "9c199ad8f281f20154fc258fe41a6814"
        act.password == "258fe4149c199ad8f2811a68f20154fc"
    }

    def "Read tls for client"() {
        setup:
        def yaml =
                "tls:\n" +
                        "  ca: /etc/ca.myservice.com.crt\n" +
                        "  certificate: /etc/client1.myservice.com.crt\n" +
                        "  key: /etc/client1.myservice.com.key"
        when:
        def act = reader.readClientTls(reader.readNode(yaml))
        then:
        act != null
        act.ca == "/etc/ca.myservice.com.crt"
        act.certificate == "/etc/client1.myservice.com.crt"
        act.key == "/etc/client1.myservice.com.key"
    }

    def "Read tls for server"() {
        setup:
        def yaml =
                "tls:\n" +
                        "  enabled: true\n" +
                        "  server:\n" +
                        "    certificate: \"/etc/client1.myservice.com.crt\"\n" +
                        "    key: \"/etc/client1.myservice.com.key\"\n" +
                        "  client:\n" +
                        "    require: false\n" +
                        "    ca: /etc/ca.myservice.com.crt"
        when:
        def act = reader.readServerTls(reader.readNode(yaml))
        then:
        act != null
        act.enabled != null
        act.enabled
        act.certificate == "/etc/client1.myservice.com.crt"
        act.key == "/etc/client1.myservice.com.key"
        act.clientRequire != null
        !act.clientRequire
        act.clientCa == "/etc/ca.myservice.com.crt"
    }
}
