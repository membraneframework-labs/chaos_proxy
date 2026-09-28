# ChaosProxy

[![Hex.pm](https://img.shields.io/hexpm/v/chaos_proxy.svg)](https://hex.pm/packages/chaos_proxy)
[![API Docs](https://img.shields.io/badge/api-docs-yellow.svg?style=flat)](https://hexdocs.pm/chaos_proxy)

A seeded UDP forwarder that sits between clients and one upstream and behaves
like a bad link, each direction on its own: blackout, random loss, a rate
limit with a tail-drop queue, and delay. It knows nothing about what it
carries; QUIC is encrypted and addressed by connection ID, so forwarding
datagrams is transparent to it. It was written to put congestion between MoQ
relays, publishers and subscribers.

```
client  ──up──▶  proxy  ──▶  upstream
client  ◀─down─  proxy  ◀──  upstream
```

This package is LLM-generated.

## Installation

```elixir
def deps do
  [
    {:chaos_proxy, "~> 0.1.0"}
  ]
end
```

## Usage

```elixir
alias ChaosProxy.{Config, Impairment}

config = Config.new!(upstream_host: "127.0.0.1", upstream_port: 4443, seed: 7)
{:ok, proxy} = ChaosProxy.start_link(config)
port = ChaosProxy.port(proxy)   # point the clients here

# A bottleneck in front of subscribers: the downlink is shaped, its delay and
# blackouts apply to the uplink too.
:ok = ChaosProxy.apply(proxy, %Impairment{rate_kbps: 700, queue_ms: 200, delay_ms: 10})

# A publisher on a thin uplink; the direction left out keeps its setting.
:ok = ChaosProxy.apply(proxy, up: %Impairment{rate_kbps: 500, loss_pct: 1})

%{seconds: seconds, totals: %{up: up, down: down}, clients: 1} = ChaosProxy.report(proxy)
```

Each direction applies, in order: `blackout?` (drop everything), `loss_pct`
(seeded random loss), `rate_kbps` (token bucket; `:infinity` for none) with
`queue_ms` of queueing before tail drop, then `delay_ms`. The default
`%Impairment{}` is a transparent link.

Which side is which comes from who dials: the clients send to the proxy's
port, so a subscriber's media arrives on the downlink and a publisher's
leaves on the uplink. Both directions are shared by all clients behind a
proxy, as one access link would be; for a bottleneck per client, start a
proxy per client.

`report/1` has counters per direction for every second (the last `:history`
of them, 300 by default) and in total: offered and forwarded bytes, tail-drop,
loss, blackout and refused (over `:max_clients`) packet counts, and the peak
queue.

`ChaosProxy.Link` is the shaper on its own, as plain data with the time passed
in, for driving it from a simulation or a test without sockets.

The options are documented in `ChaosProxy.Config`.

## Fidelity

- Timing has millisecond granularity: delays and bucket pacing are
  `Process.send_after/3` timers. Each direction keeps its packets in order.
- The bucket holds at least three and the queue at least eight 1500-byte
  packets, whatever the rate, so full-size datagrams still pass a very slow
  link.
- There is no jitter, reordering, duplication or corruption.
- Losses repeat for a seed; packet timing does not, so a repeated run is
  close, not identical.
- One process forwards everything. On an Apple M3 Pro, one client sending
  1200-byte datagrams got 1.6 Gbit/s through a transparent proxy and
  800 Mbit/s through a rate-limited one with 20 ms delay, with nothing lost in
  the proxy.

## License

Copyright 2026, [Software Mansion](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=chaos_proxy)

Licensed under the [Apache License, Version 2.0](LICENSE)
