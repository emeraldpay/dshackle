package io.emeraldpay.dshackle.testing.trial.rinkeby

import io.emeraldpay.dshackle.testing.trial.ProxyClient
import spock.lang.IgnoreIf
import spock.lang.Specification

@IgnoreIf({ System.getProperty('trialMode') != 'rinkeby' })
class CallContractSpec extends Specification {

    def client = ProxyClient.ethereumRinkeby()

    def "get block"() {
        when:
        def act = client.execute(65, "eth_call",
                [
                        [
                                "to"  : "0xB8Ba177465b3d696180742c6ba58C408175CB6Dd",
                                "data": "0x70a08231000000000000000000000000da3f87aa38d07f1fc836873a3b34ec23088ef78a"
                        ],
                        "0xc90f1c8c125a4d5b90742f16947bdb1d10516f173fd7fc51223d10499de2a812"
                ]
        )
        then:
        act.id == 65
        act.result != null
        act.result == "0x000000000000000000000000000000000000000000000000000000003af9a800"
        act.error == null
    }

}
