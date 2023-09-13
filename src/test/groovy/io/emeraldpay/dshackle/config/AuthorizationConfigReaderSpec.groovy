package io.emeraldpay.dshackle.config

import spock.lang.Specification

class AuthorizationConfigReaderSpec extends Specification {
    def reader = new AuthorizationConfigReader()

    def "if no auth option then default settings"() {
        setup:
        def yamlIs = this.class.getClassLoader().getResourceAsStream("configs/upstreams-basic.yaml")
        when:
        def act = reader.read(yamlIs)
        then:
        act == AuthorizationConfig.default()
    }

    def "if auth is disabled then defaults settings"() {
        setup:
        def yamlIs = this.class.getClassLoader().getResourceAsStream("configs/auth-disabled.yaml")
        when:
        def act = reader.read(yamlIs)
        then:
        act == AuthorizationConfig.default()
    }

    def "check default settings"() {
        when:
        def act = AuthorizationConfig.default()
        then:
        !act.enabled
        act.serverConfig.externalPublicKeyPath == ""
        act.serverConfig.providerPrivateKeyPath == ""
    }

    def "exceptions if no settings"() {
        setup:
        def yamlIs = this.class.getClassLoader().getResourceAsStream(filePath)
        when:
        reader.read(yamlIs)
        then:
        def t = thrown(IllegalStateException)
        t.message == message
        where:
        filePath                                | message
        "configs/auth-without-public-key.yaml"  | "External key in not specified"
        "configs/auth-without-private-key.yaml" | "Private key in not specified"
        "configs/auth-without-key-pair.yaml"    | "Auth keys is not specified"
        "configs/auth-without-key-owner.yaml"   | "Public key owner in not specified"
        "configs/auth-with-wrong-priv-key.yaml" | "There is no such file: classpath:keys/priv-wrong.p8.key"
        "configs/auth-with-wrong-pub-key.yaml"  | "There is no such file: classpath:keys/pub-wrong.key"
        "configs/auth-without-any-config.yaml"  | "Token auth server settings are not specified"
    }

    def "client settings is correct"() {
        setup:
        def yamlIs = this.class.getClassLoader().getResourceAsStream("configs/auth-with-client-settings.yaml")
        when:
        def act = reader.read(yamlIs)
        then:
        act.clientConfig == new AuthorizationConfig.ClientConfig("classpath:keys/priv-wrong.p8.key")
    }
}
