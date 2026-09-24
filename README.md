# This is an LLM-generated PoC that we currently use to simulate congestion control of MoQ pipelines. Feedback is welcome!

# ChaosProxy

A seeded UDP forwarder that sits between clients and one upstream and behaves
like a bad link. It knows nothing about what it carries; QUIC is encrypted and
addressed by connection ID, so forwarding datagrams is transparent to it.

Extracted from `moq_fuzz` (which derived it from `moq_chaos_relay`'s proxy);
both now use it as a path dependency:

```elixir
{:chaos_proxy, path: Path.expand("../chaos_proxy", __DIR__)}
```

## What it does

```
client  ──uplink──▶  proxy  ─────────▶  upstream (relay)
client  ◀─downlink─  proxy  ◀─────────  upstream
```

- **Downlink** (upstream → client): random loss, then a token bucket at
  `rate_kbps` feeding a FIFO of `queue_ms` with tail drop, then `delay_ms`.
- **Uplink** (client → upstream): `delay_ms` only.
- **Blackout**: everything dropped, both ways.

Random decisions come from a PRNG seeded at start. Counters are kept per
second and cumulatively (`report/1`).

```elixir
{:ok, proxy} = ChaosProxy.start_link(relay_port: 4443, seed: 7)
port = ChaosProxy.port(proxy)
:ok = ChaosProxy.apply(proxy, %ChaosProxy.Impairment{rate_kbps: 700, queue_ms: 200, delay_ms: 10})
%{seconds: _, totals: _, clients: _} = ChaosProxy.report(proxy)
```

See the moduledoc of `ChaosProxy` for the options (`:history` and
`:client_idle_ms` matter in a long-running process).

## Open design question: publisher → relay and relay → subscriber

Today only the downlink is shaped, which is the relay → subscriber case when
subscribers are the clients. Whether a publisher-side proxy should be this
module or another one:

**The forwarding is already symmetric.** At the UDP level a publisher and a
subscriber are the same thing: a QUIC client that dials the relay. Put a
publisher behind this proxy today and it works; what is wrong is only that the
direction carrying the media (now the *uplink*) gets delay and blackouts but no
rate limit, queue or loss. Nothing about the socket handling, client table,
seeding or counters would differ. So a second module would be a copy with the
two `handle_info({:udp, ...})` clauses swapped.

**What is asymmetric is one pipeline stage, not the proxy.** The shaper (loss →
token bucket → queue → delay) with its own state (`queue`, `queue_bytes`,
`tokens`, `refilled_at`, `drain_timer`, rng, counters) is the unit. The proposal
is to pull that into a `ChaosProxy.Link` struct with `offer/3`, `drain/1` and
`apply/2`, and give the proxy two of them, `up` and `down`, each with its own
`Impairment`:

```elixir
ChaosProxy.apply(proxy, down: %Impairment{rate_kbps: 700})            # subscriber-side, as today
ChaosProxy.apply(proxy, up: %Impairment{rate_kbps: 700})              # publisher-side
ChaosProxy.apply(proxy, up: uplink, down: downlink)                   # an asymmetric access link
ChaosProxy.apply(proxy, %Impairment{...})                             # shorthand: down, with the delay mirrored up (today's behaviour)
```

One module, one process per proxied hop. Reasons to prefer this over two
modules:

- A real access link degrades both directions at once, with different rates
  (ADSL/LTE uplink ≪ downlink). Congestion on the *ACK path* of a subscriber, or
  on the *feedback path* of a publisher, is a case worth generating, and it needs
  both shapers in one proxy.
- The fuzzer's scenario stays one schedule of link changes per hop instead of
  two kinds of proxy with two vocabularies.
- Counters per direction fall out for free, and the oracle needs them: "the
  publisher's uplink was congested in this window" excuses different things than
  "the subscriber's downlink was".

Reasons one might still want separation, and why they do not hold here:

- *Different defaults or semantics per role.* The roles differ in the harness
  (who dials through the proxy, what the oracle excuses), not in the proxy. That
  belongs in the caller: `moq_fuzz` would start one proxy per hop it wants to
  degrade and label them `:publisher_link` / `:subscriber_link`.
- *Per-client impairment* (three subscribers, one of them on a bad link) is a
  separate axis. It is solved by one proxy instance per client group, which
  already works because each instance has its own port, not by a module per role.

What does need care when the uplink becomes shaped:

- **Per-direction queues must be independent**; one shared token bucket would
  couple the directions and no real link does that (except half-duplex Wi-Fi,
  which is out of scope).
- **Blackout** stays a property of the whole link (both directions), separate
  from the per-direction impairments, since a one-way blackout is a different
  and rarer failure (it could be added as `loss_pct: 100.0` in one direction).
- **Per-client vs shared queue.** The downlink queue is shared by all clients
  behind the proxy, which models one bottleneck in front of several subscribers.
  For publishers the natural model is one bottleneck per publisher; with one
  proxy per publisher that is what you get, so no per-client queues are needed.
- **The relay's ingress has never been degraded** in either project. Expect new
  behaviour: moq-relay as a *subscriber* of the publisher runs the same client
  model that produced moq_fuzz finding #1.

Not proposed: a relay ↔ relay (cluster) proxy is the same module again, with
the downstream relay as the "client".
