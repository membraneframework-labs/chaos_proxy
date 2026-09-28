defmodule ChaosProxy.Config do
  @moduledoc """
  What `ChaosProxy` runs, built with `new!/1`:

      config = ChaosProxy.Config.new!(upstream_port: 4443, seed: 7)
      config.upstream_host  #=> {127, 0, 0, 1}

  A config from `new!/1` has its options checked and the upstream resolved.
  `ChaosProxy` expects such a config; a hand-built `%Config{}` is not checked.

  ## Options

    * `:upstream_port` (required), `:upstream_host` (default `"127.0.0.1"`) -
      a host name or an IPv4 or IPv6 address
    * `:listen_ip` - address clients send to, default `{127, 0, 0, 1}`
    * `:listen_port` - default 0, a port the OS picks; read it back with
      `ChaosProxy.port/1`
    * `:impairment` - the initial one, in any form `ChaosProxy.apply/2` takes;
      default transparent
    * `:seed` - PRNG seed, default 0
    * `:history` - how many per-second buckets `ChaosProxy.report/1` keeps,
      default 300; `:infinity` keeps them all
    * `:client_idle_ms` - a client silent this long in both directions is
      forgotten and its upstream socket closed, default 60 000; `:infinity`
      keeps every client
    * `:max_clients` - how many clients the proxy keeps at once, default
      `:infinity`. Beyond it, idle ones are forgotten first; if none is, a new
      client's packets are dropped and counted as `refused_packets`. Each
      client holds an upstream socket, so a proxy on a public port wants one.
    * `:name` - a name to register the proxy's process under (default `nil`)
  """

  alias ChaosProxy.Impairment

  @typedoc "A number of something, or no bound on it."
  @type limit(number) :: number | :infinity

  @type option ::
          {:upstream_host, :inet.ip_address() | :inet.hostname() | String.t()}
          | {:upstream_port, :inet.port_number()}
          | {:listen_ip, :inet.ip_address()}
          | {:listen_port, :inet.port_number()}
          | {:impairment, ChaosProxy.impairments()}
          | {:seed, integer()}
          | {:history, limit(pos_integer())}
          | {:client_idle_ms, limit(non_neg_integer())}
          | {:max_clients, limit(pos_integer())}
          | {:name, GenServer.name() | nil}

  @type t :: %__MODULE__{
          upstream_host: :inet.ip_address(),
          upstream_port: :inet.port_number(),
          listen_ip: :inet.ip_address(),
          listen_port: :inet.port_number(),
          impairment: ChaosProxy.impairments(),
          seed: integer(),
          history: limit(pos_integer()),
          client_idle_ms: limit(non_neg_integer()),
          max_clients: limit(pos_integer()),
          name: GenServer.name() | nil
        }

  @enforce_keys [:upstream_port]
  defstruct upstream_host: "127.0.0.1",
            upstream_port: nil,
            listen_ip: {127, 0, 0, 1},
            listen_port: 0,
            impairment: [],
            seed: 0,
            history: 300,
            client_idle_ms: 60_000,
            max_clients: :infinity,
            name: nil

  @doc """
  Builds a config from options, resolving `:upstream_host`.

  Raises `KeyError` for an unknown option and `ArgumentError` for a missing
  `:upstream_port`, a value the proxy cannot run with or a host that does not
  resolve.
  """
  @spec new!([option()]) :: t()
  def new!(opts) when is_list(opts) do
    %__MODULE__{} = config = struct!(__MODULE__, opts)

    check!(config, :upstream_port, config.upstream_port in 1..65_535, "a port")
    check!(config, :listen_port, config.listen_port in 0..65_535, "a port or 0")
    check!(config, :listen_ip, :inet.is_ip_address(config.listen_ip), "an IP address tuple")
    check!(config, :impairment, impairments?(config.impairment), "an Impairment or :up/:down")
    check!(config, :seed, is_integer(config.seed), "an integer")
    check!(config, :history, limit?(config.history, 1), "a positive integer or :infinity")
    check!(config, :max_clients, limit?(config.max_clients, 1), "a positive integer or :infinity")

    check!(
      config,
      :client_idle_ms,
      limit?(config.client_idle_ms, 0),
      "a non-negative integer or :infinity"
    )

    %__MODULE__{config | upstream_host: resolve!(config)}
  end

  # The struct holds the options as given, not yet a `t()`.
  @spec check!(struct(), atom(), boolean(), String.t()) :: :ok
  defp check!(_config, _key, true, _expected), do: :ok

  defp check!(config, key, false, expected) do
    raise ArgumentError,
          "#{inspect(key)} must be #{expected}, got: #{inspect(Map.fetch!(config, key))}"
  end

  @spec limit?(term(), non_neg_integer()) :: boolean()
  defp limit?(:infinity, _min), do: true
  defp limit?(value, min), do: is_integer(value) and value >= min

  @spec impairments?(term()) :: boolean()
  defp impairments?(%Impairment{}), do: true

  defp impairments?(impairments) when is_list(impairments),
    do:
      Enum.all?(
        impairments,
        &match?({direction, %Impairment{}} when direction in [:up, :down], &1)
      )

  defp impairments?(_other), do: false

  @spec resolve!(struct()) :: :inet.ip_address()
  defp resolve!(%__MODULE__{upstream_host: ip} = config) when is_tuple(ip) do
    check!(config, :upstream_host, :inet.is_ip_address(ip), "a host name or an IP address")
    ip
  end

  defp resolve!(%__MODULE__{upstream_host: host} = config) do
    check!(
      config,
      :upstream_host,
      is_binary(host) or is_atom(host) or is_list(host),
      "a host name or an IP address"
    )

    name = to_charlist(host)

    with {:error, _v4} <- :inet.getaddr(name, :inet),
         {:error, reason} <- :inet.getaddr(name, :inet6) do
      raise ArgumentError, "cannot resolve upstream #{name}: #{inspect(reason)}"
    else
      {:ok, ip} -> ip
    end
  end
end
