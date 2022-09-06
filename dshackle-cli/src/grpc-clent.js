const grpc = require("@grpc/grpc-js");
const path = require('path')
const protoLoader = require("@grpc/proto-loader");

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

export function describe(url, handler) {
    const client = new emerald.Blockchain(
        url,
        grpc.credentials.createInsecure()
    );
    console.log('Connecting to: ' + url + '...')
    client.Describe({}, handler);
}
