# Sunrise Tech Check — Beacons

Regional probe servers for the [Sunrise Tech Check](https://github.com/sunriseproductions/techcheck-client) desktop client. Each beacon is a small Go binary deployed at one of Sunrise's regional points of presence. The client tests against every beacon to measure latency, throughput, jitter, MTU, clock skew, UDP reachability, and DNS timing against each region.

## Wire protocol

The client expects these endpoints from each beacon:

| Method  | Path                       | Purpose                                 |
|---------|----------------------------|-----------------------------------------|
| `GET`   | `/healthz`                 | Liveness check                          |
| `GET`   | `/preflight/ping`          | Empty 200; used as a TCP latency target |
| `GET`   | `/preflight/mtu`           | 1400-byte payload for path-MTU probing  |
| `GET`   | `/preflight/echo-ts`       | Server clock for skew measurement       |
| `GET`   | `/preflight/download/10mb` | 10 MB random payload for throughput     |
| `POST`  | `/preflight/upload`        | Discards up to 5 MB; for upload throughput |
| `GET`   | `/preflight/whoami`        | Returns client IP, POP ID, region label |
| UDP     | `:4172`, `:8443` (echo)    | Magic-prefixed echo for UDP RTT/jitter  |

Bandwidth-related endpoints are gated by a small in-process semaphore that replies `503 Retry-After: 5` when busy.

nginx also accepts HTTPS on TCP `8443` (same certificate and rules as `443`), the port Amazon DCV uses.

### UDP stream test

On either UDP port, a client can request a stream of sequenced packets to measure loss, jitter and reordering. Each packet starts with the magic and a one-byte type:

- `H` hello (client): version, nonce, rate, packet size, duration.
- `C` cookie (beacon): nonce and a cookie bound to the client address.
- `S` start (client): the hello body plus the cookie. Only a valid cookie gets a stream, so a spoofed source cannot aim traffic at a third party.
- `B` busy (beacon): retry-after seconds.
- `D` data (beacon): sequence number, send timestamp, padding.
- `E` end (beacon): total packets sent.

Caps: 40 Mbit/s, 15 s, packet size 200-1400 bytes. A stream takes a slot from the same semaphore as the HTTP bandwidth endpoints. The beacon replies `B` when no slot is free.

## Build

```bash
make build-beacon         # Linux x86_64 binary at build/beacon
go test ./...             # unit tests
```

## Deploy

The `deploy/ansible/` role provisions a beacon node on Ubuntu: nginx (TLS via Let's Encrypt), a systemd unit for the beacon binary, and a small per-host config file. See `deploy/ansible/README.md` for the operator runbook.

A sanitised `deploy/ansible/inventory.example.yml` shows the inventory shape. Real beacon hosts and SSH keys live in the operator's private layer — they are not committed here.

## License

MIT. See [LICENSE](LICENSE).
