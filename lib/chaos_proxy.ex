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

  A proxy is started with its `t:options/0`:

      {:ok, proxy} = ChaosProxy.start_link(upstream_port: 4443, seed: 7)
      ChaosProxy.port(proxy)  # point the clients here

  Under a supervisor:

      {ChaosProxy, upstream_port: 4443, name: MyApp.Proxy}

  What the link does not model is in the README's
  [Fidelity](README.md#fidelity) section.
  """

  use GenServer

  require Logger

  alias ChaosProxy.{Clients, Impairment, Link, Options, Stats}

  @bucket_ms 1_000

  # Kernel receive buffer of every socket, so that a burst is dropped (and
  # counted) by the shaper rather than silently by the kernel in front of it.
  # The OS default can be small: often ~208 KB on Linux.
  @recbuf 4_000_000

  @typedoc """
  The options of a proxy to start.

    * `:upstream_port` (required), `:upstream_host` (default `"127.0.0.1"`) -
      where the proxy forwards to; the host is a name or an IPv4 or IPv6
      address, as a string or a tuple, and is resolved as the proxy starts.
    * `:listen_ip` - the address clients send to (default `{127, 0, 0, 1}`).
    * `:listen_port` - the port clients send to (default `0`, one the OS
      picks); read it back with `port/1`.
    * `:impairment` - the initial impairments, in any form `apply/2` takes
      (default `[]`, a transparent link).
    * `:seed` - makes the loss and the jitter repeatable (default `0`).
    * `:history` - how many seconds `report/1` goes back (default `300`);
      `:infinity` keeps them all.
    * `:client_idle_ms` - a client silent this long in both directions is
      forgotten (default `60_000`); `:infinity` keeps every client.
    * `:max_clients` - how many clients the proxy keeps at once (default
      `:infinity`). Beyond it, idle ones are forgotten first; if none is, a
      new client's packets are refused. A proxy on a public port wants a
      limit.
    * `:name` - a name to register the proxy's process under (none by
      default).
  """
  @type options :: [option()]

  @type option ::
          {:upstream_port, :inet.port_number()}
          | {:upstream_host, :inet.ip_address() | :inet.hostname() | String.t()}
          | {:listen_ip, :inet.ip_address()}
          | {:listen_port, :inet.port_number()}
          | {:impairment, impairments()}
          | {:seed, integer()}
          | {:history, limit(pos_integer())}
          | {:client_idle_ms, limit(non_neg_integer())}
          | {:max_clients, limit(pos_integer())}
          | {:name, GenServer.name()}

  @typedoc "A number of something, or no bound on it."
  @type limit(number) :: number | :infinity

  @typedoc """
  Why the proxy did not start: its options failed validation, or the port
  clients send to could not be opened, as in `{:listen, :eaddrinuse}`.
  """
  @type start_error :: {:validation_failed, String.t()} | {:listen, :inet.posix()}

  @type direction :: :up | :down

  @typedoc """
  What one direction did with its packets:

    * `offered_bytes` - arrived at the proxy
    * `forwarded_bytes`, `forwarded_packets` - left it, after their delay
    * `dropped_packets` - did not fit the queue
    * `lost_packets` - taken by loss, random or in bursts
    * `blackout_packets` - arrived during a blackout
    * `queue_bytes_max` - the most that waited for the rate limit at once
    * `refused_packets` - came from a client the proxy did not take, being at
      `:max_clients` or out of sockets; on the uplink only
  """
  @type counters :: %{
          offered_bytes: non_neg_integer(),
          forwarded_bytes: non_neg_integer(),
          forwarded_packets: non_neg_integer(),
          dropped_packets: non_neg_integer(),
          lost_packets: non_neg_integer(),
          blackout_packets: non_neg_integer(),
          queue_bytes_max: non_neg_integer(),
          refused_packets: non_neg_integer()
        }

  @typedoc """
  The counters of one second, counted from the proxy's start. Seconds may be
  missing from a report: do not assume they are consecutive.
  """
  @type second :: %{second: non_neg_integer(), up: counters(), down: counters()}

  @type impairments ::
          Impairment.t() | [{:up, Impairment.t()} | {:down, Impairment.t()}]

  defmodule State do
    @moduledoc false

    alias ChaosProxy.{Clients, Link, Options, Stats}

    # A pending `{:drain, direction, token}` and when it fires.
    @type timer :: {reference(), token :: reference(), at_ms :: number()}

    @type t :: %__MODULE__{
            options: Options.t(),
            listen: :gen_udp.socket(),
            started_at: float(),
            links: %{up: Link.t(), down: Link.t()},
            timers: %{up: timer() | nil, down: timer() | nil},
            stats: Stats.t(),
            clients: Clients.t()
          }

    @enforce_keys [:options, :listen, :started_at, :links, :stats]
    defstruct @enforce_keys ++ [timers: %{up: nil, down: nil}, clients: %Clients{}]
  end

  # Addressed to the client's upstream socket on the way up, to the client on
  # the way down.
  @typep packet :: {:gen_udp.socket() | Clients.client(), binary()}

  @spec child_spec(options()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    %{
      id: {__MODULE__, Keyword.get(opts, :name) || make_ref()},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @spec start_link(options()) ::
          {:ok, pid()} | {:error, start_error() | {:already_started, pid()}}
  def start_link(opts) when is_list(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))

  @spec start(options()) :: {:ok, pid()} | {:error, start_error() | {:already_started, pid()}}
  def start(opts) when is_list(opts),
    do: GenServer.start(__MODULE__, opts, name: Keyword.get(opts, :name))

  @spec stop(GenServer.server(), timeout()) :: :ok
  def stop(proxy, timeout \\ 5_000), do: GenServer.stop(proxy, :normal, timeout)

  @doc "The UDP port clients send to."
  @spec port(GenServer.server()) :: :inet.port_number()
  def port(proxy), do: GenServer.call(proxy, :port)

  @doc """
  Changes the impairments.

  A keyword list with `:up` and/or `:down` sets those directions and leaves
  the other as it is. A bare `Impairment` sets the downlink and gives the
  uplink only its `delay_ms`, `jitter_ms` and `blackout?`: a bottleneck in
  front of the clients, with the delay of the path back.

  Raises `ArgumentError` for impairments that fail validation, leaving the
  proxy as it was.
  """
  @spec apply(GenServer.server(), impairments()) :: :ok
  def apply(proxy, impairments) do
    case Impairment.parse(impairments) do
      {:ok, impairments} -> GenServer.call(proxy, {:apply, impairments})
      {:error, message} -> raise ArgumentError, message
    end
  end

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
  def init(opts) do
    with {:ok, options} <- parse(opts),
         {:ok, listen} <- listen(options) do
      now = now_ms()

      {:ok,
       %State{
         options: options,
         listen: listen,
         started_at: now,
         links: %{
           down: Link.new(options.impairment.down, options.seed, now),
           up: Link.new(options.impairment.up, options.seed + 1, now)
         },
         stats: Stats.new(options.history)
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @spec parse(options()) :: {:ok, Options.t()} | {:error, {:validation_failed, String.t()}}
  defp parse(opts) do
    with {:error, message} <- Options.parse(opts), do: {:error, {:validation_failed, message}}
  end

  @spec listen(Options.t()) :: {:ok, :gen_udp.socket()} | {:error, {:listen, :inet.posix()}}
  defp listen(options) do
    opts = [
      :binary,
      family(options.listen_ip),
      active: true,
      ip: options.listen_ip,
      recbuf: @recbuf
    ]

    with {:error, reason} <- :gen_udp.open(options.listen_port, opts),
         do: {:error, {:listen, reason}}
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
    |> add(direction, :forwarded_packets, length(due))
    |> schedule(direction, wait_ms, now)
  end

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

  @spec deliver(State.t(), direction(), packet()) :: :ok | {:error, term()}
  defp deliver(state, :up, {upstream, data}) do
    %Options{upstream_host: ip, upstream_port: port} = state.options
    _result = :gen_udp.send(upstream, ip, port, data)
  end

  defp deliver(state, :down, {{ip, port}, data}) do
    _result = :gen_udp.send(state.listen, ip, port, data)
  end

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
    opts = [:binary, family(state.options.upstream_host), active: true, recbuf: @recbuf]

    with false <- full?(state),
         {:ok, upstream} <- :gen_udp.open(0, opts) do
      {:ok, upstream, seen(state, client, upstream, now)}
    else
      true ->
        {:refused, state}

      # Refused rather than crashing, which would drop every client.
      {:error, reason} ->
        Logger.warning("ChaosProxy refused a client, no upstream socket: #{inspect(reason)}")
        {:refused, state}
    end
  end

  @spec seen(State.t(), Clients.client(), :gen_udp.socket(), float()) :: State.t()
  defp seen(state, client, upstream, now),
    do: %{state | clients: Clients.put(state.clients, client, upstream, now)}

  @spec full?(State.t()) :: boolean()
  defp full?(%State{options: %Options{max_clients: :infinity}}), do: false
  defp full?(state), do: Clients.size(state.clients) >= state.options.max_clients

  @spec expire_clients(State.t(), float()) :: State.t()
  defp expire_clients(state, now) do
    {sockets, clients} = Clients.expire(state.clients, state.options.client_idle_ms, now)
    Enum.each(sockets, &:gen_udp.close/1)
    %{state | clients: clients}
  end

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
