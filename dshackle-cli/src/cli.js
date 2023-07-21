import {describe, connect, nativeCall} from "./grpc-clent";
import arg from 'arg';
import clc from "cli-color";
import util from "util";

const path = require('path')
const protoLoader = require("@grpc/proto-loader");
const grpc = require("@grpc/grpc-js");

const options = {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
};

const PROTO_PATH = path.join(__dirname, "../../emerald-grpc/proto/blockchain.proto");
const packageDefinition = protoLoader.loadSync(PROTO_PATH, options);
const emerald = grpc.loadPackageDefinition(packageDefinition).emerald

export function cli(args) {
    let opts = parseArgumentsIntoOptions(args);
    if (!opts.url) {
        console.log("Err: URL not specified!!!")
        return
    }

    const chains = mapChains()

    const client = connect(opts.url, opts.ca, opts.cert, opts.key, emerald)
    describe(client, (error, response) => {
        if (error) {
            console.error(clc.red('Connection to ' + opts.url + ' failed:  ' + error.message));
            return
        }
        console.log(clc.green('Connected to ', opts.url));
        if (opts.print) {
            console.log(util.inspect(response, false, null, true /* enable colors */));
        }
        processDescribe(client, response, opts.testRun, chains)
    })
}

function processDescribe(client, response, testRun, chains) {
    let promises = []
    let statuses = new Map()

    response.chains.forEach((item) => {
        const state = item.status.availability
        const chain = item.status.chain
        let status = {
            state: 'AVAIL_UNKNOWN',
            grpc: clc.yellow('UNKNOWN'),
            failed: false
        }

        switch (state) {
            case 'AVAIL_OK':
                status.state = clc.green(state);
                break
            case 'AVAIL_UNKNOWN':
            case 'AVAIL_UNAVAILABLE':
                status.state = clc.red(state);
                break
            default:
                status.state = clc.yellow(state);
        }

        if (state === 'AVAIL_OK') {
            promises.push(nativeCall(client, chains.get(chain), chain))
        } else {
            status.failed = true
        }
        statuses.set(chain, status)
    })

    Promise.all(promises).then(responses => {
        responses.forEach((resp) => {
            let status = statuses.get(resp.chain)
            if (resp.error) {
                status.failed = true
                status.grpc = clc.red(resp.error.message)
            } else {
                if (resp.payload.succeed) {
                    status.grpc = clc.green('OK')
                } else {
                    status.grpc = clc.red('FAILED')
                    status.failed = true
                }
            }
        })

        let hasError = false
        statuses.forEach((status, chain) => {
            printState(chain, status)
            if (status.failed) {
                hasError = true
            }
        })

        if (hasError && testRun) {
            process.exit(1)
        }
    })
}

function mapChains() {
    return new Map(
        emerald.ChainRef.type.value.map(obj => {
            return [obj.name, obj.number]
        })
    )
}

function printState(chain, status) {
    console.log(chain + ' -> ' + "state: " + clc.bold(status.state) + " gRPC: " + clc.bold(status.grpc))
}

function parseArgumentsIntoOptions(rawArgs) {
    const args = arg(
        {
            '--print': Boolean,
            '--test-run': Boolean,
            '--ca': String,
            '--cert': String,
            '--key': String,
            '-p': '--print'
        },
        {
            argv: rawArgs.slice(2),
        }
    );
    return {
        print: args['--print'] || false,
        testRun: args['--test-run'] || false,
        url: args._[0],
        ca: args['--ca'],
        cert: args['--cert'],
        key: args['--key']
    };
}


