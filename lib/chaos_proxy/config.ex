defmodule ChaosProxy.Config do
  @moduledoc """
  What `ChaosProxy` runs, built with `new!/1`:

      config = ChaosProxy.Config.new!(upstream_port: 4443, seed: 7)
      config.upstream_host  #=> {127, 0, 0, 1}

  `ChaosProxy` expects a config from `new!/1`, not a hand-built `%Config{}`.
  Options are not checked against their types, see `t:option/0`.

  ## Options

    * `:upstream_port` (required), `:upstream_host` (default `"127.0.0.1"`) -
      a host name or an IPv4 or IPv6 address
    * `:listen_ip` - address clients send to, default `{127, 0, 0, 1}`
    * `:listen_port` - default 0, a port the OS picks; read it back with
      `ChaosProxy.port/1`
    * `:impairment` - the initial one, in any form `ChaosProxy.apply/2` takes;
      default transparent
    * `:seed` - makes the random loss repeatable, default 0
    * `:history` - how many seconds `ChaosProxy.report/1` goes back, default
      300; `:infinity` keeps them all
    * `:client_idle_ms` - a client silent this long in both directions is
      forgotten, default 60 000; `:infinity` keeps every client
    * `:max_clients` - how many clients the proxy keeps at once, default
      `:infinity`. Beyond it, idle ones are forgotten first; if none is, a new
      client's packets are refused. A proxy on a public port wants a limit.
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
          impairment: %{up: Impairment.t(), down: Impairment.t()},
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
  Builds a config from options.

  Raises `KeyError` for an unknown option and `ArgumentError` for a missing
  `:upstream_port`, a host that does not resolve, a `:history` of 0 or an
  impairment `ChaosProxy.apply/2` would refuse.
  """
  @spec new!([option()]) :: t()
  def new!(opts) when is_list(opts) do
    %__MODULE__{} = config = struct!(__MODULE__, opts)

    if config.history == 0,
      do: raise(ArgumentError, ":history must be a positive integer or :infinity, got: 0")

    transparent = %{up: %Impairment{}, down: %Impairment{}}

    %__MODULE__{
      config
      | upstream_host: resolve!(config.upstream_host),
        impairment: Map.merge(transparent, Impairment.split(config.impairment))
    }
  end

  @spec resolve!(:inet.ip_address() | :inet.hostname() | String.t()) :: :inet.ip_address()
  defp resolve!(ip) when is_tuple(ip), do: ip

  defp resolve!(host) do
    name = to_charlist(host)

    with {:error, _v4} <- :inet.getaddr(name, :inet),
         {:error, reason} <- :inet.getaddr(name, :inet6) do
      raise ArgumentError, "cannot resolve upstream #{name}: #{inspect(reason)}"
    else
      {:ok, ip} -> ip
    end
  end
end
