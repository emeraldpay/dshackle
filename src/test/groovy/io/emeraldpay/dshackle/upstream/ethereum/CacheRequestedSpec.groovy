package io.emeraldpay.dshackle.upstream.ethereum

import io.emeraldpay.dshackle.cache.Caches
import io.emeraldpay.dshackle.data.DefaultContainer
import spock.lang.Specification

class CacheRequestedSpec extends Specification {

    def "Do nothing if unsupported method"() {
        setup:
        def caches = Mock(Caches)
        CacheRequested instance = new CacheRequested(caches)
        when:
        instance.onReceive("eth_hashrate", [], '"0x38a"'.bytes)
        then:
        0 * caches._(*_)
    }

    def "Caches tx receipt"() {
        setup:
        def caches = Mock(Caches)
        CacheRequested instance = new CacheRequested(caches)
        def json = '''{
            "blockHash": "0x2c3cfd4c7f2b58859371f5795eaf8524caa6e63145ac7e9df23c8d63aab891ae",
            "blockNumber": "0x213b8a",
            "contractAddress": null,
            "cumulativeGasUsed": "0x5208",
            "gasUsed": "0x5208",
            "logs": [],
            "transactionHash": "0x5929b36be4586c57bd87dfb7ea6be3b985c1f527fa3d69d221604b424aeb4197",
            "transactionIndex": "0x00"
        }'''.bytes

        when:
        instance.onReceive("eth_getTransactionReceipt", ["0x5929b36be4586c57bd87dfb7ea6be3b985c1f527fa3d69d221604b424aeb4197"], json)

        then:
        1 * caches.cacheReceipt(Caches.Tag.REQUESTED, { DefaultContainer it ->
            it.height == 0x213b8a &&
                    it.txId.toHex() == "5929b36be4586c57bd87dfb7ea6be3b985c1f527fa3d69d221604b424aeb4197" &&
                    it.json == json
        })
    }

}
