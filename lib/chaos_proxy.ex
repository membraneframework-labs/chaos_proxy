defmodule ChaosProxy do
  @moduledoc """
  A seeded UDP forwarder between clients and one upstream that behaves like a
  bad link, shaping each direction on its own.

      client  ──up──▶  proxy  ──▶  upstream
      client  ◀─down─  proxy  ◀──  upstream

  Each direction is a `ChaosProxy.Link` with its own `ChaosProxy.Impairment`:
  blackout, random loss, a token bucket feeding a tail-drop queue, then a
  fixed delay. Clients are whoever sends to the proxy's port; each gets its
  own upstream socket, so the upstream sees one peer per client. Both
  directions are shared by all clients, as one access link in front of them
  would be; a bottleneck per client takes a proxy per client.

  It knows nothing about what it carries. QUIC is encrypted and addressed by
  connection ID, so forwarding datagrams is transparent to it.

      {:ok, proxy} = ChaosProxy.start_link(upstream_port: 4443, seed: 7)
      port = ChaosProxy.port(proxy)

      # the downlink only, its delay and blackout mirrored on the uplink
      :ok = ChaosProxy.apply(proxy, %ChaosProxy.Impairment{rate_kbps: 700, delay_ms: 10})

      # each direction on its own, e.g. a thin uplink in front of a publisher
      :ok = ChaosProxy.apply(proxy, up: %ChaosProxy.Impairment{rate_kbps: 500}, down: %ChaosProxy.Impairment{})

  Every random decision comes from a PRNG seeded at start, so the losses of a
  run repeat (packet timing does not, so a repeat is close, not identical).
  Counters are kept per direction, per second and in total (`report/1`).

  ## Options

    * `:upstream_port` (required), `:upstream_host` (default `"127.0.0.1"`) -
      a host name or an IPv4 or IPv6 address
    * `:listen_ip` - address clients send to, default `{127, 0, 0, 1}`
    * `:listen_port` - default 0, read it back with `port/1`
    * `:impairment` - the initial one, in any form `apply/2` takes; default
      transparent
    * `:seed` - PRNG seed, default 0
    * `:history` - how many per-second buckets `report/1` keeps, default 300;
      `:infinity` keeps them all
    * `:client_idle_ms` - a client silent this long in both directions is
      forgotten and its upstream socket closed, default 60 000; `:infinity`
      keeps every client
    * `:max_clients` - how many clients the proxy keeps at once, default
      `:infinity`. Beyond it, idle ones are forgotten first; if none is, a new
      client's packets are dropped and counted as `refused_packets`. Each
      client holds an upstream socket, so a proxy on a public port wants one.
    * `:name` - registered name, optional

  ## Fidelity

  One process forwards everything, and the bucket and the delay are paced by
  `Process.send_after/3` timers, so timing has millisecond granularity and
  throughput is bounded by what one process can forward (see the README).
  Each direction delivers in the order packets arrived: there is no jitter,
  reordering, duplication or corruption.
  """

  use GenServer

  alias ChaosProxy.{Impairment, Link}

  @bucket_ms 1_000

  # Kernel receive buffer of every socket, so that a burst is dropped (and
  # counted) by the shaper rather than silently by the kernel in front of it.
  # The OS default can be small: often ~208 KB on Linux.
  @recbuf 4_000_000

  @type direction :: :up | :down

  @type counters :: %{
          offered_bytes: non_neg_integer(),
          forwarded_bytes: non_neg_integer(),
          dropped_packets: non_neg_integer(),
          lost_packets: non_neg_integer(),
          blackout_packets: non_neg_integer(),
          queue_bytes_max: non_neg_integer(),
          refused_packets: non_neg_integer()
        }

  @type second :: %{second: non_neg_integer(), up: counters(), down: counters()}

  @type impairments ::
          Impairment.t() | [{:up, Impairment.t()} | {:down, Impairment.t()}]

  @typep counter ::
           :offered_bytes
           | :forwarded_bytes
           | :dropped_packets
           | :lost_packets
           | :blackout_packets
           | :queue_bytes_max
           | :refused_packets

  defmodule State do
    @moduledoc false

    alias ChaosProxy.Link

    @type client :: {:inet.ip_address(), :inet.port_number()}

    # Addressed to the client's upstream socket on the way up, to the client on
    # the way down.
    @type packet :: {:gen_udp.socket() | client(), binary()}

    @type per_direction(value) :: %{up: value, down: value}

    @type t :: %__MODULE__{
            listen: :gen_udp.socket(),
            upstream: {:inet.ip_address(), :inet.port_number()},
            links: per_direction(Link.t()),
            drain_timers: per_direction(reference() | nil),
            delayed: per_direction(:queue.queue({number(), packet()})),
            delay_timers: per_direction(reference() | nil),
            started_at: float(),
            history: pos_integer() | :infinity,
            client_idle_ms: non_neg_integer() | :infinity,
            max_clients: pos_integer() | :infinity,
            totals: per_direction(ChaosProxy.counters()),
            clients: %{client() => %{socket: :gen_udp.socket(), seen_ms: float()}},
            upstreams: %{:gen_udp.socket() => client()},
            current: ChaosProxy.second() | nil,
            seconds: [ChaosProxy.second()]
          }

    @enforce_keys [
      :listen,
      :upstream,
      :links,
      :started_at,
      :history,
      :client_idle_ms,
      :max_clients,
      :totals
    ]
    defstruct @enforce_keys ++
                [
                  drain_timers: %{up: nil, down: nil},
                  delayed: %{up: :queue.new(), down: :queue.new()},
                  delay_timers: %{up: nil, down: nil},
                  clients: %{},
                  upstreams: %{},
                  current: nil,
                  seconds: []
                ]
  end

  @typep client :: State.client()
  @typep packet :: State.packet()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))

  @doc "The UDP port clients send to."
  @spec port(GenServer.server()) :: :inet.port_number()
  def port(proxy), do: GenServer.call(proxy, :port)

  @doc """
  Changes the impairments. A keyword list with `:up` and/or `:down` sets
  those directions and leaves the other as it is. A bare `Impairment` sets
  the downlink and gives the uplink only its `delay_ms` and `blackout?`, a
  bottleneck in front of the clients with the delay of the path back.
  """
  @spec apply(GenServer.server(), impairments()) :: :ok
  def apply(proxy, impairments), do: GenServer.call(proxy, {:apply, split(impairments)})

  @doc """
  Per-second counters for each direction, oldest first (the last `:history`
  of them, the current second included), totals since start, and the number
  of live clients.
  """
  @spec report(GenServer.server()) :: %{
          seconds: [second()],
          totals: %{up: counters(), down: counters()},
          clients: non_neg_integer()
        }
  def report(proxy), do: GenServer.call(proxy, :report)

  @impl true
  def init(opts) do
    listen_ip = Keyword.get(opts, :listen_ip, {127, 0, 0, 1})

    {:ok, listen} =
      :gen_udp.open(Keyword.get(opts, :listen_port, 0), [
        :binary,
        family(listen_ip),
        active: true,
        ip: listen_ip,
        recbuf: @recbuf
      ])

    upstream_ip = resolve!(Keyword.get(opts, :upstream_host, "127.0.0.1"))
    seed = Keyword.get(opts, :seed, 0)
    now = now_ms()

    %{up: up, down: down} =
      Map.merge(
        %{up: %Impairment{}, down: %Impairment{}},
        split(Keyword.get(opts, :impairment, []))
      )

    {:ok,
     %State{
       listen: listen,
       upstream: {upstream_ip, Keyword.fetch!(opts, :upstream_port)},
       links: %{down: Link.new(down, seed, now), up: Link.new(up, seed + 3, now)},
       started_at: now,
       history: Keyword.get(opts, :history, 300),
       client_idle_ms: Keyword.get(opts, :client_idle_ms, 60_000),
       max_clients: Keyword.get(opts, :max_clients, :infinity),
       totals: %{up: empty_counters(), down: empty_counters()}
     }}
  end

  @impl true
  def handle_call(:port, _from, state) do
    {:ok, port} = :inet.port(state.listen)
    {:reply, port, state}
  end

  def handle_call({:apply, impairments}, _from, state) do
    now = now_ms()

    state =
      Enum.reduce(impairments, state, fn {direction, impairment}, state ->
        state.links[direction]
        |> update_in(&Link.apply(&1, impairment, now))
        |> drain(direction)
      end)

    {:reply, :ok, state}
  end

  def handle_call(:report, _from, state) do
    state = state |> rotate() |> expire_clients()
    seconds = Enum.reverse([state.current | state.seconds])
    {:reply, %{seconds: seconds, totals: state.totals, clients: map_size(state.clients)}, state}
  end

  @impl true
  def handle_info({:udp, listen, ip, port, data}, %State{listen: listen} = state) do
    client = {ip, port}

    case upstream(state, client) do
      {:ok, upstream, state} ->
        state = touch(state, client, upstream)
        {:noreply, offer(state, :up, {upstream, data}, byte_size(data))}

      {:full, state} ->
        {:noreply, count(state, :up, :refused_packets, 1)}
    end
  end

  def handle_info({:udp, upstream, _ip, _port, data}, state) do
    case state.upstreams do
      %{^upstream => client} ->
        state = touch(state, client, upstream)
        {:noreply, offer(state, :down, {client, data}, byte_size(data))}

      # Left in the mailbox by a socket closed for an expired client.
      _expired ->
        {:noreply, state}
    end
  end

  def handle_info({:drain, direction}, state) do
    {:noreply, drain(put_in(state.drain_timers[direction], nil), direction)}
  end

  def handle_info({:delayed, direction}, state) do
    {:noreply, flush(put_in(state.delay_timers[direction], nil), direction)}
  end

  @spec offer(State.t(), direction(), packet(), non_neg_integer()) :: State.t()
  defp offer(state, direction, packet, size) do
    state = count(state, direction, :offered_bytes, size)
    {outcome, link} = Link.offer(state.links[direction], packet, size, now_ms())
    state = put_in(state.links[direction], link)

    case outcome do
      :queued -> state |> count(direction, :queue_bytes_max, 0) |> drain(direction)
      :blackout -> count(state, direction, :blackout_packets, 1)
      :lost -> count(state, direction, :lost_packets, 1)
      :dropped -> count(state, direction, :dropped_packets, 1)
    end
  end

  @spec drain(State.t(), direction()) :: State.t()
  defp drain(state, direction) do
    now = now_ms()
    {released, link, wait_ms} = Link.drain(state.links[direction], now)
    state = put_in(state.links[direction], link)
    due = now + link.impairment.delay_ms

    state =
      Enum.reduce(released, state, fn {_to, data} = packet, state ->
        state.delayed[direction]
        |> update_in(&:queue.in({due, packet}, &1))
        |> count(direction, :forwarded_bytes, byte_size(data))
      end)

    state =
      if wait_ms != nil and state.drain_timers[direction] == nil do
        timer = Process.send_after(self(), {:drain, direction}, wait_ms)
        put_in(state.drain_timers[direction], timer)
      else
        state
      end

    flush(state, direction)
  end

  # The delay line: one FIFO and one timer per direction, so packets leave in
  # the order they were released. A shorter delay does not overtake packets
  # still held for a longer one.
  @spec flush(State.t(), direction()) :: State.t()
  defp flush(state, direction) do
    now = now_ms()
    timer = state.delay_timers[direction]

    case :queue.peek(state.delayed[direction]) do
      {:value, {due, packet}} when due <= now ->
        deliver(state, direction, packet)

        state.delayed[direction]
        |> update_in(&:queue.drop/1)
        |> flush(direction)

      {:value, {due, _packet}} when timer == nil ->
        timer = Process.send_after(self(), {:delayed, direction}, ceil(due - now))
        put_in(state.delay_timers[direction], timer)

      _empty_or_waiting ->
        state
    end
  end

  # The upstream socket may have been closed for an expired client meanwhile.
  @spec deliver(State.t(), direction(), packet()) :: :ok | {:error, term()}
  defp deliver(state, :up, {upstream, data}) do
    {ip, port} = state.upstream
    _result = :gen_udp.send(upstream, ip, port, data)
  end

  defp deliver(state, :down, {{ip, port}, data}) do
    _result = :gen_udp.send(state.listen, ip, port, data)
  end

  # The client's upstream socket, opened for a new client unless the proxy is
  # at `:max_clients` even after forgetting idle ones.
  @spec upstream(State.t(), client()) :: {:ok, :gen_udp.socket(), State.t()} | {:full, State.t()}
  defp upstream(state, client) do
    case state.clients do
      %{^client => %{socket: upstream}} ->
        {:ok, upstream, state}

      _new ->
        state = if full?(state), do: expire_clients(state), else: state

        if full?(state) do
          {:full, state}
        else
          {upstream_ip, _port} = state.upstream

          {:ok, upstream} =
            :gen_udp.open(0, [:binary, family(upstream_ip), active: true, recbuf: @recbuf])

          {:ok, upstream, %{state | upstreams: Map.put(state.upstreams, upstream, client)}}
        end
    end
  end

  @spec full?(State.t()) :: boolean()
  defp full?(%State{max_clients: :infinity}), do: false
  defp full?(state), do: map_size(state.clients) >= state.max_clients

  @spec split(impairments()) :: %{optional(direction()) => Impairment.t()}
  defp split(%Impairment{} = down),
    do: %{down: down, up: %Impairment{delay_ms: down.delay_ms, blackout?: down.blackout?}}

  defp split(impairments) when is_list(impairments), do: Map.new(impairments)

  @spec resolve!(:inet.ip_address() | :inet.hostname() | String.t()) :: :inet.ip_address()
  defp resolve!(ip) when is_tuple(ip), do: ip

  defp resolve!(host) do
    host = to_charlist(host)

    with {:error, _v4} <- :inet.getaddr(host, :inet),
         {:error, reason} <- :inet.getaddr(host, :inet6) do
      raise ArgumentError, "cannot resolve upstream #{host}: #{inspect(reason)}"
    else
      {:ok, ip} -> ip
    end
  end

  @spec family(:inet.ip_address()) :: :inet | :inet6
  defp family(ip) when tuple_size(ip) == 8, do: :inet6
  defp family(_ip), do: :inet

  @spec now_ms() :: float()
  defp now_ms, do: System.monotonic_time(:microsecond) / 1_000

  ## Clients

  @spec touch(State.t(), client(), :gen_udp.socket()) :: State.t()
  defp touch(state, client, upstream),
    do: %{state | clients: Map.put(state.clients, client, %{socket: upstream, seen_ms: now_ms()})}

  # A client that went away leaves its upstream socket behind; checked when a
  # second rolls over and on `report/1`.
  @spec expire_clients(State.t()) :: State.t()
  defp expire_clients(%State{client_idle_ms: :infinity} = state), do: state

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

  @spec count(State.t(), direction(), counter(), non_neg_integer()) :: State.t()
  defp count(state, direction, key, amount) do
    state = rotate(state)

    bump =
      case key do
        :queue_bytes_max ->
          queued = Link.queue_bytes(state.links[direction])
          &Map.update!(&1, key, fn peak -> max(peak, queued) end)

        _sum ->
          &Map.update!(&1, key, fn sum -> sum + amount end)
      end

    state = update_in(state.current[direction], bump)
    update_in(state.totals[direction], bump)
  end

  # Moves on to the current second's bucket, archiving the previous one.
  @spec rotate(State.t()) :: State.t()
  defp rotate(state) do
    second = trunc((now_ms() - state.started_at) / @bucket_ms)

    case state.current do
      %{second: ^second} ->
        state

      nil ->
        %{state | current: empty_second(second)}

      current ->
        expire_clients(%{state | seconds: keep(current, state), current: empty_second(second)})
    end
  end

  @spec keep(second(), State.t()) :: [second()]
  defp keep(second, %State{history: :infinity, seconds: seconds}), do: [second | seconds]

  defp keep(second, %State{history: n, seconds: seconds}),
    do: Enum.take([second | seconds], n - 1)

  @spec empty_second(non_neg_integer()) :: second()
  defp empty_second(second), do: %{second: second, up: empty_counters(), down: empty_counters()}

  @spec empty_counters() :: counters()
  defp empty_counters,
    do: %{
      offered_bytes: 0,
      forwarded_bytes: 0,
      dropped_packets: 0,
      lost_packets: 0,
      blackout_packets: 0,
      queue_bytes_max: 0,
      refused_packets: 0
    }
end
