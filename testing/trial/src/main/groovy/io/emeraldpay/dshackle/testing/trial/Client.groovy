package io.emeraldpay.dshackle.testing.trial

interface Client {
    Map<String, Object> execute(String method, List<Object> params)
    Map<String, Object> execute(Object id, String method, List<Object> params)
}