const grpc = require("@grpc/grpc-js");
const fs = require('fs');

var id = 100

export function connect(url, ca, cert, key, emerald) {
    let credentials = grpc.credentials.createInsecure()
    if (ca || cert || key) {
        console.log("Using TLS")
        credentials = grpc.credentials.createSsl(
            ca ? fs.readFileSync(ca) : null,
            key ? fs.readFileSync(key) : null,
            cert ? fs.readFileSync(cert) : null
        );
    }
    console.log('Connecting to: ' + url + '...')
    return new emerald.Blockchain(
        url,
        credentials
    );
}

export function describe(client, handler) {
    client.Describe({}, handler);
}

export function nativeCall(client, chainCode, chain) {
    return new Promise((resolve, reject) => {
        const call = client.NativeCall({
            chain: chainCode,
            items: [{
                id: id++,
                method: "eth_getBalance",
                payload: "WyIweDhEOTc2ODlDOTgxODg5MkI3MDBlMjdGMzE2Y2MzRTQxZTE3ZkJlYjkiLCAibGF0ZXN0Il0="
            }],
            quorum: 1,
            min_availability: 0
        })
        call.on('data', (item) => {
            resolve(toResult(chain, item, null))
        })
        call.on('end', () => resolve(toResult(chain, null, null)))
        call.on('error', (e) => reject(toResult(chain, null, e)))
    })
}

function toResult(chain, obj, err) {
    return {
        chain: chain,
        payload: obj,
        error: err
    }
}
