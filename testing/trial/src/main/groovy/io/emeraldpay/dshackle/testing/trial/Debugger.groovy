package io.emeraldpay.dshackle.testing.trial

class Debugger {

    public static enabled = true

    static void showOut(String json) {
        if (!enabled) {
            return
        }
        println(">> $json")
    }

    static void showIn(String json) {
        if (!enabled) {
            return
        }
        println("<< $json")
    }
}
