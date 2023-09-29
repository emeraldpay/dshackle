package io.emeraldpay.dshackle.config.spans

import io.emeraldpay.dshackle.config.MainConfig
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.impl.client.HttpClients
import org.bouncycastle.openssl.PEMParser
import org.springframework.cloud.sleuth.zipkin2.ZipkinRestTemplateCustomizer
import org.springframework.http.HttpRequest
import org.springframework.http.client.ClientHttpRequestExecution
import org.springframework.http.client.ClientHttpRequestInterceptor
import org.springframework.http.client.ClientHttpResponse
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestTemplate
import java.io.ByteArrayOutputStream
import java.io.StringReader
import java.nio.file.Files
import java.nio.file.Paths
import java.security.KeyFactory
import java.security.KeyStore
import java.security.SecureRandom
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.util.zip.GZIPOutputStream
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext

@Component
class ZipkinSSLCustomizer(private val mainConfig: MainConfig) : ZipkinRestTemplateCustomizer {
    override fun customizeTemplate(restTemplate: RestTemplate): RestTemplate {
        return if (mainConfig.tls?.enabled == true) {
            setupSSL(mainConfig.tls!!.certificate!!, mainConfig.tls!!.key!!)
        } else {
            restTemplate
        }.apply {
            interceptors.add(0, GZipInterceptor())
        }
    }

    private fun setupSSL(certPath: String, privateKeyPath: String): RestTemplate {
        // Load the certificate
        val certificateReader = StringReader(Files.readString(Paths.get(certPath)))
        val pemObject = PEMParser(certificateReader).readPemObject()
        val certificate = CertificateFactory.getInstance("X.509").generateCertificate(pemObject.content.inputStream()) as X509Certificate
        // Load the private key
        val privateKeyReader = StringReader(Files.readString(Paths.get(privateKeyPath)))
        val pemKeyPair = PEMParser(privateKeyReader).readPemObject()
        val privKeySpec = PKCS8EncodedKeySpec(pemKeyPair.content)
        val privateKey = KeyFactory.getInstance("RSA").generatePrivate(privKeySpec)

        // Create the key store
        val keyStore = KeyStore.getInstance(KeyStore.getDefaultType()).apply {
            load(null, null)
            setCertificateEntry("certificate", certificate)
            setKeyEntry("private-key", privateKey, null, arrayOf(certificate))
        }

        // Create the SSL context
        val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()).apply {
            init(keyStore, null)
        }
        val sslContext = SSLContext.getInstance("TLS").apply {
            init(keyManagerFactory.keyManagers, null, SecureRandom())
        }

        // Create the HTTP client
        val socketFactory = SSLConnectionSocketFactory(sslContext)
        val httpClient = HttpClients.custom().setSSLSocketFactory(socketFactory).build()

        // Create the request factory
        val requestFactory = HttpComponentsClientHttpRequestFactory(httpClient)

        // Create the RestTemplate
        return RestTemplate(requestFactory)
    }

    private class GZipInterceptor : ClientHttpRequestInterceptor {
        override fun intercept(
            request: HttpRequest,
            body: ByteArray,
            execution: ClientHttpRequestExecution,
        ): ClientHttpResponse {
            request.headers.add("Content-Encoding", "gzip")
            val gzipped = ByteArrayOutputStream()
            GZIPOutputStream(gzipped).use { compressor -> compressor.write(body) }
            return execution.execute(request, gzipped.toByteArray())
        }
    }
}
