# ChaosProxy

[![Hex.pm](https://img.shields.io/hexpm/v/chaos_proxy.svg)](https://hex.pm/packages/chaos_proxy)
[![API Docs](https://img.shields.io/badge/api-docs-yellow.svg?style=flat)](https://hexdocs.pm/chaos_proxy)

A seeded UDP forwarder that sits between clients and one upstream and behaves
like a bad link, each direction on its own: blackout, random loss, a rate
limit with a queue, and delay. It knows nothing about what it carries; QUIC is
encrypted and addressed by connection ID, so forwarding datagrams is
transparent to it. It was written to put congestion between MoQ relays,
publishers and subscribers.

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

config = Config.new!(upstream_port: 4443, seed: 7)
{:ok, proxy} = ChaosProxy.start_link(config)
port = ChaosProxy.port(proxy)   # point the clients here

:ok = ChaosProxy.apply(proxy, %Impairment{rate_kbps: 700, queue_ms: 200, delay_ms: 10})
:ok = ChaosProxy.apply(proxy, up: %Impairment{rate_kbps: 500, loss_pct: 1})

%{seconds: seconds, totals: %{up: up, down: down}, clients: 1} = ChaosProxy.report(proxy)
```

`ChaosProxy` covers the directions, the clients and the counters,
`ChaosProxy.Impairment` the knobs and `ChaosProxy.Config` the options.
`ChaosProxy.Link` is one direction on its own, without sockets or timers, for
a simulation or a test.

## Fidelity

- Timing has millisecond granularity, and each direction keeps its packets in
  order.
- There is no jitter, reordering, duplication or corruption.
- Losses repeat for a seed; packet timing does not, so a repeated run is
  close, not identical.
- On an Apple M3 Pro, one client sending 1200-byte datagrams got 1.6 Gbit/s
  through a transparent proxy and 800 Mbit/s through a rate-limited one with
  20 ms delay, with nothing lost in the proxy.

## License

Copyright 2026, [Software Mansion](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=chaos_proxy)

Licensed under the [Apache License, Version 2.0](LICENSE)
