package io.emeraldpay.dshackle.auth.service

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.util.ResourceUtils
import java.math.BigInteger
import java.security.interfaces.RSAPrivateKey
import java.security.interfaces.RSAPublicKey

class RsaKeyReaderTest {
    private val rsaKeyReader = RsaKeyReader()

    @Test
    fun `read rsa keys`() {
        val privPath = ResourceUtils.getFile("classpath:keys/priv.p8.key").path
        val publicPath = ResourceUtils.getFile("classpath:keys/public-drpc.pem").path

        val pair = rsaKeyReader.getKeyPair(privPath, publicPath)
        val privateKey = pair.providerPrivateKey
        val publicKey = pair.externalPublicKey

        assertEquals("RSA", publicKey.algorithm)
        assertEquals("RSA", privateKey.algorithm)
        assertTrue(privateKey is RSAPrivateKey)
        assertTrue(publicKey is RSAPublicKey)
        assertTrue {
            (publicKey as RSAPublicKey)
                .run {
                    val pubExponent = BigInteger("65537")
                    val pubModulus = BigInteger(
                        "240721495118071395463408682378448427874712549978328672120875120395" +
                            "572530650526403138656868181996853461531503006596703750232991335166031612653183175739344513002793" +
                            "13348833980409910131805866311837600488070189526246817317791758685747539443624701130779843529258916" +
                            "650627570641542946773123818063304222185205897480168513227047010620610697362185901253798594085716467" +
                            "92700927695491574061920135297311257486773262784524717857554194798974994838378321973611865011031457" +
                            "8859834631052697211398759550437148562864384448640344034211059348047816397935347753413946137935402" +
                            "562264627177210569215727831448291731093608051334025332988696191",
                    )
                    publicExponent == pubExponent && modulus == pubModulus
                }
        }
        assertTrue {
            (privateKey as RSAPrivateKey)
                .run {
                    val privModulus = BigInteger(
                        "2832492054027911929186769713106999105750517776225145627897719982164048887089826085864021820" +
                            "93695282142899084731750598834879176944012613634073603363270499693159806868257221336886781437442" +
                            "7025894179697109350195776864078690751332857203663624730020413376539574511375327958483292435380" +
                            "2072047577218574632871804997852794886795318485439021501388394533101191305015940756335545719496" +
                            "04557847518214006893286271137213491059994279470633688916032797849945887259137876037521925382454" +
                            "762238089597912107394417801381736627011587603418078578130206830219321107525729614610300336784925" +
                            "6741484379041499791605663418628739419306248595872949",
                    )
                    val privExponent = BigInteger(
                        "2704777724417882789147602456408780048462378556719441898242139736695157047955437327090684858" +
                            "9022442508913912020206014886681429805316382175637569992035330289450733224635356250217508515675" +
                            "1746547003301653260530619509144105809847739245799589488158044325999968951997941276374874356761" +
                            "7744591423894125208163912446945277960899800043458394321204754817284255735206518583285355872405" +
                            "3966258052672146605639969648388744960700258048094135244952249403403074548577386549705289154261" +
                            "2346391566526533514196717633725503640437930121313225102536030460301806443109607461162231878852" +
                            "64869837629637060803692577216204745443259318662678133769",
                    )

                    privateExponent == privExponent && modulus == privModulus
                }
        }
    }
}
