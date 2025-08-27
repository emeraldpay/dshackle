package io.emeraldpay.dshackle.upstream.signature

import io.emeraldpay.dshackle.upstream.Upstream
import org.apache.commons.codec.binary.Hex
import java.security.MessageDigest
import java.security.Signature
import java.security.interfaces.ECPrivateKey

class EcdsaSigner(
    private val privateKey: ECPrivateKey,
    val keyId: Long,
) : ResponseSigner {
    companion object {
        const val SIGN_SCHEME = "SHA256withECDSA"
        const val MSG_PREFIX = "DSHACKLESIG"
        const val MSG_SEPARATOR = '/'
    }

    override fun sign(
        nonce: Long,
        message: ByteArray,
        source: Upstream,
    ): ResponseSigner.Signature {
        // Java 16 has removed SECP256K1, see https://bugs.openjdk.org/browse/JDK-8251547
        // So use Bounce Castle provider ("BC") as a default provider
        val sig = Signature.getInstance(SIGN_SCHEME, "BC")
        sig.initSign(privateKey)
        val wrapped = wrapMessage(nonce, message, source)
        sig.update(wrapped.toByteArray())
        val value = sig.sign()
        return ResponseSigner.Signature(
            value,
            source.getId(),
            keyId,
        )
    }

    /**
     * To avoid various attacks, such as various kinds of padding and message alternating attacks,
     * we (1) tag the original message to specify the source, (2) ensure no parts of the message can affect each other
     * and (3) ensure the message cannot break the wrapping.
     *
     * We're doing that by converting the message as `"DSHACKLESIG/" || str(nonce) || "/" || hex(sha256(msg))`
     *
     * I.e.:
     * - three elements in the wrapped message
     * - separated by "/" which is not a part of any element
     * - first element is DSHACKLESIG tag
     * - second is the nonce value encode as decimal string
     * - third is SHA256 hash of the original message encoded as hex string
     */
    fun wrapMessage(
        nonce: Long,
        message: ByteArray,
        source: Upstream,
    ): String {
        val sha256 = MessageDigest.getInstance("SHA-256")
        // we create it with max capacity that we expect for the result, which is total lengths of its parts
        val formatterMsg = StringBuilder(11 + 1 + 18 + 1 + 64 + 1 + 64)
        formatterMsg
            .append(MSG_PREFIX)
            .append(MSG_SEPARATOR)
            .append(nonce.toString())
            .append(MSG_SEPARATOR)
            // We expect that the id is short enough (less than 64 symbols) and also it doesn't contain the `/` symbol
            // which is verified in UpstreamConfigReader and DefaultUpstream constructor
            .append(source.getId())
            .append(MSG_SEPARATOR)
            .append(Hex.encodeHexString(sha256.digest(message)))
        return formatterMsg.toString()
    }
}
