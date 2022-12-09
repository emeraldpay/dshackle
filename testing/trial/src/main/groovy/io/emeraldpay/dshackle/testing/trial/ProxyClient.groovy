package io.emeraldpay.dshackle.testing.trial

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.codec.binary.Hex
import org.apache.http.HttpHeaders
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.client.HttpClients

class ProxyClient implements Client {

    private int sequence = 0;

    private String url
    private ObjectMapper objectMapper

    ProxyClient(String url) {
        this.url = url
        this.objectMapper = new ObjectMapper()
    }

    static ProxyClient ethereumMock() {
        return forPrefix(18080, "eth")
    }

    static ProxyClient ethereumReal() {
        return forPrefix(18081, "eth")
    }

    static ProxyClient ethereumRinkeby() {
        return forPrefix(18081, "rinkeby")
    }

    static ProxyClient forPrefix(String prefix) {
        return forPrefix(18080, prefix)
    }

    static ProxyClient forPrefix(int port, String prefix) {
        return new ProxyClient("http://127.0.0.1:$port/$prefix")
    }

    static ProxyClient forOriginal(int port) {
        return new ProxyClient("http://127.0.0.1:$port")
    }

    Map<String, Object> execute(String method, List<Object> params) {
        return execute(sequence++, method, params)
    }

    Map<String, Object> execute(Object id, String method, List<Object> params) {
        Map jsonData = [
                id     : id,
                jsonrpc: "2.0",
                method : method,
                params : params
        ]
        String reqJson = objectMapper.writeValueAsString(jsonData)
        Debugger.showOut(reqJson)
        CloseableHttpClient httpClient = null
        try {
            httpClient = HttpClients.createDefault()
            def request = new HttpPost(url)
            request.setEntity(new StringEntity(reqJson, ContentType.APPLICATION_JSON))
            CloseableHttpResponse response = httpClient.execute(request)
            if (response.getStatusLine().statusCode != 200) {
                throw new IllegalStateException("Non-ok status: " + response.getStatusLine().statusCode)
            }
            String respJson = response.entity.content.text
            Debugger.showIn(respJson)
            return objectMapper.readerFor(Map).readValue(respJson)
        } finally {
            httpClient?.close()
        }

        return [error: "NOT EXECUTED"]
    }

}
