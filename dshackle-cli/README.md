# DSHACKLE CLI

A CLI tool to verify the [dshacle](https://github.com/emeraldpay/dshackle) installation.

## Usage
```
npx dchackle-cli-tool [-p|--print] <dshackle instance url>
```

### Options
-p | --print - will print the describe response as is

## Example

```
> dchackle-cli-tool localhost:2449
Connecting to: localhost:2449...
Connected to  localhost:2449
CHAIN_ETHEREUM -> AVAIL_OK
CHAIN_KOVAN -> AVAIL_OK
```