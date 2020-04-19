package io.emeraldpay.dshackle.upstream.bitcoin

import de.jodamob.kotlin.testrunner.OpenedClasses
import de.jodamob.kotlin.testrunner.SpotlinTestRunner
import io.emeraldpay.dshackle.test.TestingCommons
import org.junit.runner.RunWith
import org.mockserver.integration.ClientAndServer
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import spock.lang.Specification

import java.time.Duration

@RunWith(SpotlinTestRunner)
@OpenedClasses(BitcoinApi)
class BitcoinRpcHeadSpec extends Specification {

    ClientAndServer mockServer

    def "Follow 2 blocks created over 3 requests"() {
        setup:
        String hash1 = "0000000000000000000cf5a5d4dfc4347c0c1a863ec5fdb429b02b2162e50001"
        String hash2 = "0000000000000000000cf5a5d4dfc4347c0c1a863ec5fdb429b02b2162e50002"

        def block1 = """
            {
                "hash": "${hash1}",
                "confirmations": 2,
                "strippedsize": 800464,
                "size": 1591897,
                "weight": 3993289,
                "height": 626472,
                "version": 549453824,
                "versionHex": "20c00000",
                "merkleroot": "7835a26b6c9316232f8194c6b64b0a366e26a9a14e6a7e7409c34594d98a83c2",
                "tx": [],
                "time": 1587171526,
                "mediantime": 1587168930,
                "nonce": 4111305375,
                "bits": "171320bc",
                "difficulty": 14715214060656.53,
                "chainwork": "00000000000000000000000000000000000000000e9baec5f63190a2b0bbcf6b",
                "nTx": 0,
                "previousblockhash": "000000000000000000106b8cb739dd3c412613aa5d459a739794ec2572a74c10"
            }        
        """
        def block2 = """
            {
                "hash": "${hash2}",
                "confirmations": 2,
                "strippedsize": 800464,
                "size": 1591897,
                "weight": 3993289,
                "height": 626473,
                "version": 549453824,
                "versionHex": "20c00000",
                "merkleroot": "7835a26b6c9316232f8194c6b64b0a366e26a9a14e6a7e7409c34594d98a83c2",
                "tx": [],
                "time": 1587172526,
                "mediantime": 1587168930,
                "nonce": 4111305375,
                "bits": "171320bc",
                "difficulty": 14715214060656.53,
                "chainwork": "00000000000000000000000000000000000000000e9baec5f63190a2b0bbcf6c",
                "nTx": 0,
                "previousblockhash": "${hash1}"
            }        
        """

        BitcoinApi api = Mock(BitcoinApi) {
            _ * executeAndResult(_, "getbestblockhash", _, String) >>> [
                    Mono.just(hash1), Mono.just(hash1), Mono.just(hash2)
            ]
            _ * execute(_, "getblock", [hash1]) >> Mono.just(block1.bytes)
            _ * execute(_, "getblock", [hash2]) >> Mono.just(block2.bytes)
        }
        BitcoinRpcHead head = new BitcoinRpcHead(api, new ExtractBlock(TestingCommons.objectMapper()), Duration.ofMillis(200))

        when:
        def act = head.flux.take(2)
        head.start()

        then:
        StepVerifier.create(act)
                .expectNextMatches { block ->
                    block.hash.toHex() == hash1
                }.as("Block 1")
                .expectNextMatches { block ->
                    block.hash.toHex() == hash2
                }.as("Block 2")
                .expectComplete()
                .verify(Duration.ofSeconds(3))

    }
}
