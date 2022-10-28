const grpc = require("@grpc/grpc-js");
const path = require('path')
const protoLoader = require("@grpc/proto-loader");
const fs = require('fs');

const PROTO_PATH = path.join(__dirname, "../grpc/proto/blockchain.proto");

const options = {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
};

const packageDefinition = protoLoader.loadSync(PROTO_PATH, options);
const emerald = grpc.loadPackageDefinition(packageDefinition).emerald

export function describe(url, ca, cert, key, handler) {
    let credentials = grpc.credentials.createInsecure()
    if (ca || cert || key) {
        console.log("Using TLS")
        credentials = grpc.credentials.createSsl(
            ca ? fs.readFileSync(ca) : null,
            key ? fs.readFileSync(key) : null,
            cert ? fs.readFileSync(cert) : null
        );
    }
    const client = new emerald.Blockchain(
        url,
        credentials
    );

    console.log('Connecting to: ' + url + '...')
    client.Describe({}, handler);
}
