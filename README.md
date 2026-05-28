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
| UDP     | `:4172` (echo)             | Magic-prefixed echo for UDP RTT/jitter  |

Bandwidth-related endpoints are gated by a small in-process semaphore that replies `503 Retry-After: 5` when busy.

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
