/**
 * Copyright (c) 2020 ETCDEV GmbH
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
package io.emeraldpay.dshackle

import io.emeraldpay.dshackle.config.AuthConfig
import io.netty.handler.ssl.ClientAuth
import io.netty.handler.ssl.OpenSslServerContext
import io.netty.handler.ssl.SslContext
import spock.lang.Specification
import sun.security.x509.X509CertImpl

import java.security.NoSuchAlgorithmException

class TlsSetupSpec extends Specification {

    TlsSetup tlsSetup = new TlsSetup(new FileResolver(new File("src/test/resources/tls-local")))

    static {
        // getting on CI:
        //   java.security.KeyStoreException: Key protection algorithm not found: java.security.UnrecoverableKeyException: Encrypt Private Key failed: unrecognized algorithm name: PBEWithSHA1AndDESede
        //            at java.base/sun.security.pkcs12.PKCS12KeyStore.setKeyEntry(PKCS12KeyStore.java:700)
        //            at java.base/sun.security.pkcs12.PKCS12KeyStore.engineSetKeyEntry(PKCS12KeyStore.java:597)
        //            at java.base/sun.security.util.KeyStoreDelegator.engineSetKeyEntry(KeyStoreDelegator.java:111)
        //            at java.base/java.security.KeyStore.setKeyEntry(KeyStore.java:1167)
        //            at io.netty.handler.ssl.SslContext.buildKeyStore(SslContext.java:1102)
        //            at io.netty.handler.ssl.ReferenceCountedOpenSslServerContext.newSessionContext(ReferenceCountedOpenSslServerContext.java:123)
        // ----
        // see https://github.com/bcgit/bc-java/issues/941
        // https://bugs.openjdk.java.net/browse/JDK-8266279

        try {
            sun.security.x509.AlgorithmId.get("PBEWithSHA1AndDESede");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    def "TLS disabled"() {
        setup:
        def config = new AuthConfig.ServerTlsAuth(
                enabled: false
        )
        when:
        def act = tlsSetup.setupServer("test", config, false)
        then:
        act == null
    }

    def "TLS enabled"() {
        setup:
        def config = new AuthConfig.ServerTlsAuth(
                enabled: true,
                certificate: "127.0.0.1.crt",
                key: "127.0.0.1.p8.key"
        )
        when:
        def act = tlsSetup.setupServer("test", config, false)
        then:
        act != null
        act.server
        !act.client
        with((OpenSslServerContext) act) {
            act.clientAuth == ClientAuth.NONE
            with((X509CertImpl) keyCertChain[0]) {
                getIssuerDN().name == "CN=ca.myhost.dev, OU=Blockchain CA, O=My Company"
            }
        }
    }

    def "TLS enabled and required from client"() {
        setup:
        def config = new AuthConfig.ServerTlsAuth(
                enabled: true,
                certificate: "127.0.0.1.crt",
                key: "127.0.0.1.p8.key",
                clientRequire: true,
                clientCa: "ca.myhost.dev.crt"
        )
        when:
        def act = tlsSetup.setupServer("test", config, false)
        then:
        act != null
        act.server
        !act.client

        // run test on OS with OpenSSL installed
        with((OpenSslServerContext) act) {
            clientAuth == ClientAuth.REQUIRE
            with((X509CertImpl) keyCertChain[0]) {
                getIssuerDN().name == "CN=ca.myhost.dev, OU=Blockchain CA, O=My Company"
            }
        }
    }

    def "Fail if certificate not set"() {
        setup:
        def config = new AuthConfig.ServerTlsAuth(
                enabled: true,
                key: "127.0.0.1.p8.key",
        )
        when:
        tlsSetup.setupServer("test", config, false)
        then:
        def t = thrown(IllegalArgumentException)
        t.message == "Certificate not set"
    }

    def "Fail if certificate key not set"() {
        setup:
        def config = new AuthConfig.ServerTlsAuth(
                enabled: true,
                certificate: "127.0.0.1.crt"
        )
        when:
        tlsSetup.setupServer("test", config, false)
        then:
        def t = thrown(IllegalArgumentException)
        t.message == "Certificate Key not set"
    }

    def "Fail if certificate not exists"() {
        setup:
        def config = new AuthConfig.ServerTlsAuth(
                enabled: true,
                certificate: "none.crt",
                key: "127.0.0.1.p8.key",
        )
        when:
        tlsSetup.setupServer("test", config, false)
        then:
        def t = thrown(IllegalArgumentException)
    }

    def "Fail if certificate key not exists"() {
        setup:
        def config = new AuthConfig.ServerTlsAuth(
                enabled: true,
                certificate: "127.0.0.1.crt",
                key: "none.p8.key",
        )
        when:
        tlsSetup.setupServer("test", config, false)
        then:
        def t = thrown(IllegalArgumentException)
    }

    def "Fail if certificate key is invalid"() {
        setup:
        def config = new AuthConfig.ServerTlsAuth(
                enabled: true,
                certificate: "127.0.0.1.crt",
                key: "127.0.0.1.key",
        )
        when:
        tlsSetup.setupServer("test", config, false)
        then:
        def t = thrown(IllegalArgumentException)
    }

    def "Fail if client certificate not set but required"() {
        setup:
        def config = new AuthConfig.ServerTlsAuth(
                enabled: true,
                certificate: "127.0.0.1.crt",
                key: "127.0.0.1.p8.key",
                clientRequire: true
        )
        when:
        tlsSetup.setupServer("test", config, false)
        then:
        def t = thrown(IllegalArgumentException)
    }

    def "Fail if client certificate not exists"() {
        setup:
        def config = new AuthConfig.ServerTlsAuth(
                enabled: true,
                certificate: "127.0.0.1.crt",
                key: "127.0.0.1.p8.key",
                clientRequire: true,
                clientCa: "none.crt"
        )
        when:
        tlsSetup.setupServer("test", config, false)
        then:
        def t = thrown(IllegalArgumentException)
    }

    def "Fail if client certificate is invalid"() {
        setup:
        def config = new AuthConfig.ServerTlsAuth(
                enabled: true,
                certificate: "127.0.0.1.crt",
                key: "127.0.0.1.p8.key",
                clientRequire: true,
                clientCa: "ca.myhost.dev.key"
        )
        when:
        tlsSetup.setupServer("test", config, false)
        then:
        def t = thrown(IllegalArgumentException)
    }
}
