import {describe, connect, nativeCall} from "./grpc-clent";
import arg from 'arg';
import clc from "cli-color";
import util from "util";

const chains = {
    CHAIN_BSC__MAINNET: 1006,
    CHAIN_OPTIMISM__MAINNET: 1005,
    CHAIN_ARBITRUM__MAINNET: 1004,
    CHAIN_POLYGON_POS__MAINNET: 1002,
    CHAIN_ETHEREUM__MAINNET: 100
}

export function cli(args) {
    let opts = parseArgumentsIntoOptions(args);
    if (!opts.url) {
        console.log("Err: URL not specified!!!")
        return
    }

    const client = connect(opts.url, opts.ca, opts.cert, opts.key)
    describe(client, (error, response) => {
        if (error) {
            console.error(clc.red('Connection to ' + opts.url + ' failed:  ' + error.message));
            return
        }
        console.log(clc.green('Connected to ', opts.url));
        if (opts.print) {
            console.log(util.inspect(response, false, null, true /* enable colors */));
        }
        processDescribe(client, response, opts.testRun)
    })
}

function processDescribe(client, response, testRun) {
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
            promises.push(nativeCall(client, chains[chain], chain))
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


