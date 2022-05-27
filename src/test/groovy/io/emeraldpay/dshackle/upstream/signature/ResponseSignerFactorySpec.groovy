package io.emeraldpay.dshackle.upstream.signature


import io.emeraldpay.dshackle.config.SignatureConfig
import spock.lang.Specification

class ResponseSignerFactorySpec extends Specification {


    def "No signer if not enabled"() {
        setup:
        def conf = new SignatureConfig()
        when:
        def signer = new ResponseSignerFactory(conf).getObject()
        then:
        signer instanceof NoSigner
    }

    def "No signer if privkey is not configured"() {
        setup:
        def conf = new SignatureConfig()
        when:
        def signer = new ResponseSignerFactory(conf).getObject()
        then:
        signer instanceof NoSigner
    }

}
