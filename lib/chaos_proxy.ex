defmodule ChaosProxy do
  @moduledoc """
  Seeded UDP forwarder between clients and one upstream (a MoQ relay, in both
  current users), degrading the upstream -> client direction like a bottleneck
  link: a token bucket at the configured rate feeding a bounded FIFO with tail
  drop, optional random loss, a fixed one-way delay in both directions, and
  blackouts that drop everything both ways.

  Every random decision comes from a PRNG seeded at start, so the losses of a
  run repeat (packet timing does not, so a repeat is close, not identical).
  Counters are kept per second, so a caller can tell which windows were
  congested, and cumulatively.

  QUIC is encrypted and addressed by connection ID, so datagram forwarding is
  transparent to it; the proxy sees bytes, never what they carry.

  ## Options

    * `:relay_port` (required), `:relay_host` (default `"127.0.0.1"`) - the upstream
    * `:listen_port` - default 0, read it back with `port/1`
    * `:impairment` - the initial `ChaosProxy.Impairment`
    * `:seed` - PRNG seed, default 0
    * `:history` - how many per-second buckets `report/1` keeps, default
      `:infinity`; bound it in a long-running process
    * `:client_idle_ms` - a client silent this long in both directions is
      forgotten and its upstream socket closed, default 60 000; `:infinity`
      keeps every client
    * `:name` - registered name, optional
  """

  use GenServer

  alias ChaosProxy.Impairment

  @mtu 1500
  @bucket_ms 1_000

  @type second :: %{
          second: non_neg_integer(),
          offered_bytes: non_neg_integer(),
          forwarded_bytes: non_neg_integer(),
          dropped_packets: non_neg_integer(),
          lost_packets: non_neg_integer(),
          blackout_packets: non_neg_integer(),
          queue_bytes_max: non_neg_integer()
        }

  def start_link(opts) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} -> GenServer.start_link(__MODULE__, opts, name: name)
      :error -> GenServer.start_link(__MODULE__, opts)
    end
  end

  @doc "The UDP port clients connect to."
  @spec port(GenServer.server()) :: :inet.port_number()
  def port(proxy), do: GenServer.call(proxy, :port)

  @spec apply(GenServer.server(), Impairment.t()) :: :ok
  def apply(proxy, %Impairment{} = impairment), do: GenServer.call(proxy, {:apply, impairment})

  @doc """
  Per-second counters, oldest first (the last `:history` of them, the current
  second included), totals since start, and the number of live clients.
  """
  @spec report(GenServer.server()) :: %{
          seconds: [second()],
          totals: map(),
          clients: non_neg_integer()
        }
  def report(proxy), do: GenServer.call(proxy, :report)

  @impl true
  def init(opts) do
    {:ok, listen} =
      :gen_udp.open(Keyword.get(opts, :listen_port, 0), [
        :binary,
        active: true,
        ip: {127, 0, 0, 1},
        recbuf: 4_000_000
      ])

    {:ok, relay_ip} =
      :inet.getaddr(to_charlist(Keyword.get(opts, :relay_host, "127.0.0.1")), :inet)

    seed = Keyword.get(opts, :seed, 0)

    {:ok,
     %{
       listen: listen,
       relay: {relay_ip, Keyword.fetch!(opts, :relay_port)},
       config: Keyword.get(opts, :impairment, %Impairment{}),
       rng: :rand.seed_s(:exsss, {seed, seed + 1, seed + 2}),
       started_at: now_ms(),
       history: Keyword.get(opts, :history, :infinity),
       client_idle_ms: Keyword.get(opts, :client_idle_ms, 60_000),
       totals: Map.delete(empty_second(0), :second),
       clients: %{},
       upstreams: %{},
       queue: :queue.new(),
       queue_bytes: 0,
       tokens: 0.0,
       refilled_at: now_ms(),
       drain_timer: nil,
       current: nil,
       seconds: []
     }}
  end

  @impl true
  def handle_call(:port, _from, state) do
    {:ok, port} = :inet.port(state.listen)
    {:reply, port, state}
  end

  def handle_call({:apply, config}, _from, state) do
    state = %{refill(state) | config: config}
    state = %{state | tokens: min(state.tokens, burst_bytes(config))}
    {:reply, :ok, drain(state)}
  end

  def handle_call(:report, _from, state) do
    state = state |> rotate() |> expire_clients()
    seconds = Enum.reverse([state.current | state.seconds])
    {:reply, %{seconds: seconds, totals: state.totals, clients: map_size(state.clients)}, state}
  end

  # Uplink (client -> upstream): never shaped, only delayed, or lost in a blackout.
  @impl true
  def handle_info({:udp, listen, ip, port, data}, %{listen: listen} = state) do
    client = {ip, port}

    {upstream, state} =
      case state.clients do
        %{^client => %{socket: upstream}} ->
          {upstream, state}

        _none ->
          {:ok, upstream} = :gen_udp.open(0, [:binary, active: true, ip: {127, 0, 0, 1}])

          {upstream, %{state | upstreams: Map.put(state.upstreams, upstream, client)}}
      end

    state = touch(state, client, upstream)

    if state.config.blackout? do
      {:noreply, count(state, :blackout_packets, 1)}
    else
      later(state.config, {:to_relay, upstream, data})
      {:noreply, state}
    end
  end

  # Downlink (upstream -> client): blackout, random loss, then shaping.
  def handle_info({:udp, upstream, _ip, _port, data}, state) do
    case state.upstreams do
      %{^upstream => client} ->
        state = state |> touch(client, upstream) |> count(:offered_bytes, byte_size(data))

        cond do
          state.config.blackout? ->
            {:noreply, count(state, :blackout_packets, 1)}

          true ->
            {lost?, state} = lost?(state)

            if lost?,
              do: {:noreply, count(state, :lost_packets, 1)},
              else: {:noreply, shape(client, data, state)}
        end

      # Left in the mailbox by a socket closed for an expired client.
      _expired ->
        {:noreply, state}
    end
  end

  # The socket may have been closed for an expired client while this was delayed.
  def handle_info({:to_relay, upstream, data}, state) do
    {ip, port} = state.relay
    _ = :gen_udp.send(upstream, ip, port, data)
    {:noreply, state}
  end

  def handle_info({:to_client, {ip, port}, data}, state) do
    :ok = :gen_udp.send(state.listen, ip, port, data)
    {:noreply, state}
  end

  def handle_info(:drain, state), do: {:noreply, drain(%{state | drain_timer: nil})}

  defp shape(client, data, state) do
    size = byte_size(data)

    if state.queue_bytes + size > queue_cap_bytes(state.config) do
      count(state, :dropped_packets, 1)
    else
      state = %{
        state
        | queue: :queue.in({client, data}, state.queue),
          queue_bytes: state.queue_bytes + size
      }

      state |> count(:queue_bytes_max, 0) |> drain()
    end
  end

  defp drain(state) do
    state = refill(state)

    case :queue.out(state.queue) do
      {:empty, _queue} ->
        state

      {{:value, {client, data}}, rest} ->
        size = byte_size(data)

        cond do
          state.tokens >= size ->
            state = %{
              state
              | queue: rest,
                queue_bytes: state.queue_bytes - size,
                tokens: state.tokens - size
            }

            drain(deliver(client, data, state))

          state.drain_timer != nil ->
            state

          true ->
            wait_ms = ceil((size - state.tokens) / bytes_per_ms(state.config))
            %{state | drain_timer: Process.send_after(self(), :drain, max(wait_ms, 1))}
        end
    end
  end

  defp deliver(client, data, state) do
    later(state.config, {:to_client, client, data})
    count(state, :forwarded_bytes, byte_size(data))
  end

  defp later(%Impairment{delay_ms: 0}, message), do: send(self(), message)

  defp later(%Impairment{delay_ms: delay}, message),
    do: Process.send_after(self(), message, delay)

  defp lost?(%{config: %Impairment{loss_pct: pct}} = state) when pct <= 0, do: {false, state}

  defp lost?(%{config: %Impairment{loss_pct: pct}, rng: rng} = state) do
    {x, rng} = :rand.uniform_s(rng)
    {x * 100 < pct, %{state | rng: rng}}
  end

  defp refill(state) do
    now = now_ms()
    config = state.config

    tokens =
      min(state.tokens + (now - state.refilled_at) * bytes_per_ms(config), burst_bytes(config))

    %{state | tokens: tokens, refilled_at: now}
  end

  defp bytes_per_ms(%Impairment{rate_kbps: rate}), do: rate / 8

  # Enough for a few full packets, or 20 ms worth at the rate, whichever is larger.
  defp burst_bytes(config), do: max(3 * @mtu, bytes_per_ms(config) * 20)

  # `queue_ms` of queueing before tail drop, but never fewer than 8 packets.
  defp queue_cap_bytes(%Impairment{queue_ms: queue_ms} = config),
    do: max(8 * @mtu, round(bytes_per_ms(config) * queue_ms))

  defp now_ms, do: System.monotonic_time(:microsecond) / 1_000

  ## Clients

  defp touch(state, client, upstream),
    do: %{state | clients: Map.put(state.clients, client, %{socket: upstream, seen_ms: now_ms()})}

  # A client that went away leaves its upstream socket behind; checked when a
  # second rolls over and on `report/1`.
  defp expire_clients(%{client_idle_ms: :infinity} = state), do: state

  defp expire_clients(state) do
    cutoff = now_ms() - state.client_idle_ms

    {idle, live} =
      Map.split_with(state.clients, fn {_client, %{seen_ms: seen}} -> seen < cutoff end)

    upstreams =
      Enum.reduce(idle, state.upstreams, fn {_client, %{socket: socket}}, acc ->
        :gen_udp.close(socket)
        Map.delete(acc, socket)
      end)

    %{state | clients: live, upstreams: upstreams}
  end

  ## Counters

  defp count(state, key, amount) do
    state = rotate(state)

    bump =
      case key do
        :queue_bytes_max -> &Map.update!(&1, key, fn peak -> max(peak, state.queue_bytes) end)
        _ -> &Map.update!(&1, key, fn sum -> sum + amount end)
      end

    %{state | current: bump.(state.current), totals: bump.(state.totals)}
  end

  # Moves on to the current second's bucket, archiving the previous one.
  defp rotate(state) do
    second = trunc((now_ms() - state.started_at) / @bucket_ms)

    case state.current do
      nil ->
        %{state | current: empty_second(second)}

      %{second: ^second} ->
        state

      current ->
        expire_clients(%{state | seconds: keep(current, state), current: empty_second(second)})
    end
  end

  defp keep(second, %{history: :infinity, seconds: seconds}), do: [second | seconds]
  defp keep(second, %{history: n, seconds: seconds}), do: Enum.take([second | seconds], n - 1)

  defp empty_second(second),
    do: %{
      second: second,
      offered_bytes: 0,
      forwarded_bytes: 0,
      dropped_packets: 0,
      lost_packets: 0,
      blackout_packets: 0,
      queue_bytes_max: 0
    }
end
