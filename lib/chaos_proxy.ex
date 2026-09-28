defmodule ChaosProxy do
  @moduledoc """
  A seeded UDP forwarder between clients and one upstream that behaves like a
  bad link.

      client  ──up──▶  proxy  ──▶  upstream
      client  ◀─down─  proxy  ◀──  upstream

  Clients are whoever sends to the proxy's port, so a subscriber's media
  arrives on the downlink and a publisher's leaves on the uplink. The upstream
  sees one peer per client.

  Each direction has its own `ChaosProxy.Impairment`, shared by all clients as
  one access link in front of them would be; a bottleneck per client takes a
  proxy per client.

      config = ChaosProxy.Config.new!(upstream_port: 4443, seed: 7)
      {:ok, proxy} = ChaosProxy.start_link(config)
      port = ChaosProxy.port(proxy)

      :ok = ChaosProxy.apply(proxy, %ChaosProxy.Impairment{rate_kbps: 700, delay_ms: 10})
      ChaosProxy.report(proxy)

  The options are in `ChaosProxy.Config`, and what the link does not model in
  the README's [Fidelity](readme.html#fidelity) section.
  """

  use GenServer

  require Logger

  alias ChaosProxy.{Clients, Config, Impairment, Link, Stats}

  @bucket_ms 1_000

  # Kernel receive buffer of every socket, so that a burst is dropped (and
  # counted) by the shaper rather than silently by the kernel in front of it.
  # The OS default can be small: often ~208 KB on Linux.
  @recbuf 4_000_000

  @type direction :: :up | :down

  @typedoc """
  What one direction did with its packets:

    * `offered_bytes` - arrived at the proxy
    * `forwarded_bytes` - left it, after their delay
    * `dropped_packets` - did not fit the queue
    * `lost_packets` - taken by random loss
    * `blackout_packets` - arrived during a blackout
    * `queue_bytes_max` - the most that waited for the rate limit at once
    * `refused_packets` - came from a client the proxy did not take, being at
      `:max_clients` or out of sockets; on the uplink only
  """
  @type counters :: %{
          offered_bytes: non_neg_integer(),
          forwarded_bytes: non_neg_integer(),
          dropped_packets: non_neg_integer(),
          lost_packets: non_neg_integer(),
          blackout_packets: non_neg_integer(),
          queue_bytes_max: non_neg_integer(),
          refused_packets: non_neg_integer()
        }

  @typedoc """
  The counters of one second, counted from the proxy's start. There is none
  for a second nothing happened in.
  """
  @type second :: %{second: non_neg_integer(), up: counters(), down: counters()}

  @typedoc "What `apply/2` takes."
  @type impairments ::
          Impairment.t() | [{:up, Impairment.t()} | {:down, Impairment.t()}]

  defmodule State do
    @moduledoc false

    alias ChaosProxy.{Clients, Config, Link, Stats}

    # A pending `{:drain, direction, token}` and when it fires.
    @type timer :: {reference(), token :: reference(), at_ms :: number()}

    @type t :: %__MODULE__{
            config: Config.t(),
            listen: :gen_udp.socket(),
            started_at: float(),
            links: %{up: Link.t(), down: Link.t()},
            timers: %{up: timer() | nil, down: timer() | nil},
            stats: Stats.t(),
            clients: Clients.t()
          }

    @enforce_keys [:config, :listen, :started_at, :links, :stats]
    defstruct @enforce_keys ++ [timers: %{up: nil, down: nil}, clients: %Clients{}]
  end

  # Addressed to the client's upstream socket on the way up, to the client on
  # the way down.
  @typep packet :: {:gen_udp.socket() | Clients.client(), binary()}

  @spec child_spec(Config.t()) :: Supervisor.child_spec()
  def child_spec(%Config{} = config) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [config]}, type: :worker}
  end

  @doc """
  Starts a proxy linked to the caller.

  When the port clients send to cannot be opened, the error is
  `{:listen, reason}`, as in `{:error, {:listen, :eaddrinuse}}`, and a caller
  that does not trap exits exits with it, as with `GenServer.start_link/3`.
  """
  @spec start_link(Config.t()) :: GenServer.on_start()
  def start_link(%Config{} = config) do
    opts = if config.name, do: [name: config.name], else: []
    GenServer.start_link(__MODULE__, config, opts)
  end

  @doc "The UDP port clients send to."
  @spec port(GenServer.server()) :: :inet.port_number()
  def port(proxy), do: GenServer.call(proxy, :port)

  @doc """
  Changes the impairments.

  A keyword list with `:up` and/or `:down` sets those directions and leaves
  the other as it is. A bare `Impairment` sets the downlink and gives the
  uplink only its `delay_ms` and `blackout?`: a bottleneck in front of the
  clients, with the delay of the path back.

  Raises `ArgumentError` for a `rate_kbps` that is not above 0.
  """
  @spec apply(GenServer.server(), impairments()) :: :ok
  def apply(proxy, impairments),
    do: GenServer.call(proxy, {:apply, Impairment.split(impairments)})

  @doc """
  The counters of each direction: per second, oldest first and the current
  one included, and in total since the start. Also the number of clients.
  """
  @spec report(GenServer.server()) :: %{
          seconds: [second()],
          totals: %{up: counters(), down: counters()},
          clients: non_neg_integer()
        }
  def report(proxy), do: GenServer.call(proxy, :report)

  @impl true
  def init(%Config{} = config) do
    opts = [
      :binary,
      family(config.listen_ip),
      active: true,
      ip: config.listen_ip,
      recbuf: @recbuf
    ]

    case :gen_udp.open(config.listen_port, opts) do
      {:ok, listen} ->
        now = now_ms()

        {:ok,
         %State{
           config: config,
           listen: listen,
           started_at: now,
           links: %{
             down: Link.new(config.impairment.down, config.seed, now),
             up: Link.new(config.impairment.up, config.seed + 3, now)
           },
           stats: Stats.new(config.history)
         }}

      {:error, reason} ->
        {:stop, {:listen, reason}}
    end
  end

  @impl true
  def handle_call(:port, _from, state) do
    {:ok, port} = :inet.port(state.listen)
    {:reply, port, state}
  end

  def handle_call({:apply, impairments}, _from, state) do
    now = now_ms()

    state =
      Enum.reduce(impairments, tick(state, now), fn {direction, impairment}, state ->
        state.links[direction]
        |> update_in(&Link.apply(&1, impairment, now))
        |> drain(direction, now)
      end)

    {:reply, :ok, state}
  end

  def handle_call(:report, _from, state) do
    now = now_ms()
    state = state |> tick(now) |> expire_clients(now)
    report = Map.put(Stats.report(state.stats), :clients, Clients.size(state.clients))
    {:reply, report, state}
  end

  @impl true
  def handle_info({:udp, listen, ip, port, data}, %State{listen: listen} = state) do
    now = now_ms()
    state = tick(state, now)

    case upstream(state, {ip, port}, now) do
      {:ok, upstream, state} -> {:noreply, offer(state, :up, {upstream, data}, now)}
      {:refused, state} -> {:noreply, add(state, :up, :refused_packets, 1)}
    end
  end

  def handle_info({:udp, upstream, _ip, _port, data}, state) do
    case Clients.client(state.clients, upstream) do
      {:ok, client} ->
        now = now_ms()
        state = state |> seen(client, upstream, now) |> tick(now)
        {:noreply, offer(state, :down, {client, data}, now)}

      # Left in the mailbox by a socket closed for an expired client.
      :error ->
        {:noreply, state}
    end
  end

  def handle_info({:drain, direction, token}, state) do
    case state.timers[direction] do
      {_timer, ^token, _at} ->
        now = now_ms()
        state = put_in(state.timers[direction], nil)
        {:noreply, state |> tick(now) |> drain(direction, now)}

      # Sent by a timer that was replaced by an earlier one meanwhile.
      _replaced ->
        {:noreply, state}
    end
  end

  @spec offer(State.t(), direction(), packet(), float()) :: State.t()
  defp offer(state, direction, {_to, data} = packet, now) do
    size = byte_size(data)
    {outcome, link} = Link.offer(state.links[direction], packet, size)
    state = add(put_in(state.links[direction], link), direction, :offered_bytes, size)

    case outcome do
      :queued ->
        stats = Stats.peak(state.stats, direction, Link.queue_bytes(link))
        drain(%{state | stats: stats}, direction, now)

      :blackout ->
        add(state, direction, :blackout_packets, 1)

      :lost ->
        add(state, direction, :lost_packets, 1)

      :dropped ->
        add(state, direction, :dropped_packets, 1)
    end
  end

  # Sends what the link lets go of and comes back when it has more.
  @spec drain(State.t(), direction(), float()) :: State.t()
  defp drain(state, direction, now) do
    {due, link, wait_ms} = Link.drain(state.links[direction], now)
    Enum.each(due, &deliver(state, direction, &1))
    forwarded = due |> Enum.map(fn {_to, data} -> byte_size(data) end) |> Enum.sum()

    state.links[direction]
    |> put_in(link)
    |> add(direction, :forwarded_bytes, forwarded)
    |> schedule(direction, wait_ms, now)
  end

  # One timer per direction. A pending one stands unless the link is due more
  # than a millisecond before it, as after `apply/2` raised the rate.
  @spec schedule(State.t(), direction(), pos_integer() | nil, float()) :: State.t()
  defp schedule(state, _direction, nil, _now), do: state

  defp schedule(state, direction, wait_ms, now) do
    at = now + wait_ms

    case state.timers[direction] do
      {_timer, _token, pending_at} when pending_at <= at + 1 ->
        state

      pending ->
        with {timer, _token, _at} <- pending, do: Process.cancel_timer(timer)
        token = make_ref()
        timer = Process.send_after(self(), {:drain, direction, token}, wait_ms)
        put_in(state.timers[direction], {timer, token, at})
    end
  end

  # The upstream socket may have been closed for an expired client meanwhile.
  @spec deliver(State.t(), direction(), packet()) :: :ok | {:error, term()}
  defp deliver(state, :up, {upstream, data}) do
    %Config{upstream_host: ip, upstream_port: port} = state.config
    _result = :gen_udp.send(upstream, ip, port, data)
  end

  defp deliver(state, :down, {{ip, port}, data}) do
    _result = :gen_udp.send(state.listen, ip, port, data)
  end

  # The client's upstream socket, opened for a new client unless the proxy is
  # at `:max_clients` even after forgetting idle ones, or out of sockets.
  @spec upstream(State.t(), Clients.client(), float()) ::
          {:ok, :gen_udp.socket(), State.t()} | {:refused, State.t()}
  defp upstream(state, client, now) do
    case Clients.socket(state.clients, client) do
      {:ok, upstream} -> {:ok, upstream, seen(state, client, upstream, now)}
      :error -> admit(state, client, now)
    end
  end

  @spec admit(State.t(), Clients.client(), float()) ::
          {:ok, :gen_udp.socket(), State.t()} | {:refused, State.t()}
  defp admit(state, client, now) do
    state = if full?(state), do: expire_clients(state, now), else: state
    opts = [:binary, family(state.config.upstream_host), active: true, recbuf: @recbuf]

    with false <- full?(state),
         {:ok, upstream} <- :gen_udp.open(0, opts) do
      {:ok, upstream, seen(state, client, upstream, now)}
    else
      true ->
        {:refused, state}

      # The clients it has are worth more than the one it cannot take.
      {:error, reason} ->
        Logger.warning("ChaosProxy refused a client, no upstream socket: #{inspect(reason)}")
        {:refused, state}
    end
  end

  @spec seen(State.t(), Clients.client(), :gen_udp.socket(), float()) :: State.t()
  defp seen(state, client, upstream, now),
    do: %{state | clients: Clients.put(state.clients, client, upstream, now)}

  @spec full?(State.t()) :: boolean()
  defp full?(%State{config: %Config{max_clients: :infinity}}), do: false
  defp full?(state), do: Clients.size(state.clients) >= state.config.max_clients

  # A client that went away leaves its upstream socket behind; checked when a
  # second rolls over, on `report/1` and when the proxy is full.
  @spec expire_clients(State.t(), float()) :: State.t()
  defp expire_clients(state, now) do
    {sockets, clients} = Clients.expire(state.clients, state.config.client_idle_ms, now)
    Enum.each(sockets, &:gen_udp.close/1)
    %{state | clients: clients}
  end

  # Moves the counters on to the second of `now`, once per message.
  @spec tick(State.t(), float()) :: State.t()
  defp tick(state, now) do
    case Stats.rotate(state.stats, trunc((now - state.started_at) / @bucket_ms)) do
      {:same, _stats} -> state
      {:rolled, stats} -> expire_clients(%{state | stats: stats}, now)
    end
  end

  @spec add(State.t(), direction(), Stats.sum(), non_neg_integer()) :: State.t()
  defp add(state, direction, key, amount),
    do: %{state | stats: Stats.add(state.stats, direction, key, amount)}

  @spec family(:inet.ip_address()) :: :inet | :inet6
  defp family(ip) when tuple_size(ip) == 8, do: :inet6
  defp family(_ip), do: :inet

  @spec now_ms() :: float()
  defp now_ms, do: System.monotonic_time(:microsecond) / 1_000
end
