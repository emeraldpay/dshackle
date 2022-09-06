import {describe} from "./grpc-clent";
import arg from 'arg';
import clc from "cli-color";
import util from "util";

export function cli(args) {
    let opts = parseArgumentsIntoOptions(args);
    if (!opts.url) {
        console.log("Err: URL not specified!")
        return
    }
    describe(opts.url, (error, response) => {
        if (error) {
            console.error(clc.red('Connection to ' + opts.url + ' failed! [' + error.message + ']'));
        } else {
            console.error(clc.green('Connected to ', opts.url));
            if (opts.print) {
                console.log(util.inspect(response, false, null, true /* enable colors */));
            } else {
                response.chains.forEach(function (item) {
                    var state = item.status.availability;

                    switch (state) {
                        case 'AVAIL_OK':
                            state = clc.green(state);
                            break
                        case 'AVAIL_UNKNOWN':
                        case 'AVAIL_UNAVAILABLE':
                            state = clc.red(state);
                            break
                        default:
                            state = clc.yellow(state);
                    }
                    console.log(item.status.chain + ' -> ' + clc.bold(state))
                })
            }
        }
    })
}

function parseArgumentsIntoOptions(rawArgs) {
    const args = arg(
        {
            '--print': Boolean,
            '-p': '--print'
        },
        {
            argv: rawArgs.slice(2),
        }
    );
    return {
        print: args['--print'] || false,
        url: args._[0]
    };
}


