package io.emeraldpay.dshackle.upstream.ethereum.subscribe.json

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.dshackle.Global
import io.emeraldpay.etherjar.domain.Address
import io.emeraldpay.etherjar.domain.BlockHash
import io.emeraldpay.etherjar.domain.Bloom
import spock.lang.Specification

import java.time.Instant

class NewHeadSpec extends Specification {

    def "Serialize to a correct JSON"() {
        setup:
        NewHead obj = new NewHead(
                0xc7f3b4,
                BlockHash.from("0xd3b7ae1a79f5418debae9b8e9318094298c087183be0f7a0151b0e76ba38d6bc"),
                BlockHash.from("0xcda7fd1d6ee2d5da7505a0634e27f41d5ae87a344cd75bb64c1dc0863fbe9c0a"),
                Instant.ofEpochSecond(0x6128264d),
                new BigInteger("1de7f7a458cc08", 16),
                0x1ca35ef,
                0x7bb33e,
                Bloom.from("0x012040020880820356a20b8e980a2004c19f1291800501040001180bb029d0002d8c49440e002048ca00d48581000d900a458100c90139056140880582f22a0a8050224c020c233be8c3080c0a016aa4226a2001446c800822080445a2454118139804001202068401841900840c484222420c4b2022046052c0011e81a9e450085883708545810592e40040010411442300080b0130711f602880600a30c90702cb420a0102a644820650908802840810948142541404884300acc69d000840702020224c000020200880c10858418408098a61445b0ab0480234862655a5000434311b91044849c165040411aa0400b00008222642d24313020d9022219120"),
                Address.from("0x829bd824b016326a401d083b33d092293333a830"),
                null
        )
        ObjectMapper objectMapper = Global.getObjectMapper()
        def exp = '{' +
                '"number":"0xc7f3b4",' +
                '"hash":"0xd3b7ae1a79f5418debae9b8e9318094298c087183be0f7a0151b0e76ba38d6bc",' +
                '"parentHash":"0xcda7fd1d6ee2d5da7505a0634e27f41d5ae87a344cd75bb64c1dc0863fbe9c0a",' +
                '"timestamp":"0x6128264d",' +
                '"difficulty":"0x1de7f7a458cc08",' +
                '"gasLimit":"0x1ca35ef",' +
                '"gasUsed":"0x7bb33e",' +
                '"logsBloom":"0x012040020880820356a20b8e980a2004c19f1291800501040001180bb029d0002d8c49440e002048ca00d48581000d900a458100c90139056140880582f22a0a8050224c020c233be8c3080c0a016aa4226a2001446c800822080445a2454118139804001202068401841900840c484222420c4b2022046052c0011e81a9e450085883708545810592e40040010411442300080b0130711f602880600a30c90702cb420a0102a644820650908802840810948142541404884300acc69d000840702020224c000020200880c10858418408098a61445b0ab0480234862655a5000434311b91044849c165040411aa0400b00008222642d24313020d9022219120",' +
                '"miner":"0x829bd824b016326a401d083b33d092293333a830"' +
                '}'
        when:
        def json = objectMapper.writeValueAsString(obj)

        then:
        json == exp
    }
}
