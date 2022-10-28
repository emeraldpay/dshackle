# DSHACKLE CLI

A CLI tool to verify the [dshackle](https://github.com/emeraldpay/dshackle) installation.

## Usage
```
npx dshackle-cli-tool [-p|--print][--ca][--cert][--key] <dshackle instance url>
```

### Options
```-p | --print``` will print the describe response as is

### TLS
```--ca``` the root certificate data

```--cert``` the client certificate key chain, if available
```--key``` the client certificate private key, if available

## Example

```
> dshackle-cli-tool localhost:2449
Connecting to: localhost:2449...
Connected to  localhost:2449
CHAIN_ETHEREUM -> AVAIL_OK
CHAIN_KOVAN -> AVAIL_OK
```

With TLS
```
>dshackle-cli % dshackle-cli-tool --ca ./../out/ca.myhost.dev.crt --cert ./../out/client_1.crt --key ./../out/client_1.key  127.0.0.1:2450
Using TLS
Connecting to: 127.0.0.1:2450...
Connected to  127.0.0.1:2450
CHAIN_ETHEREUM -> AVAIL_OK
```