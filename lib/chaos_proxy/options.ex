defmodule ChaosProxy.Options do
  @moduledoc false

  alias ChaosProxy.Impairment

  @type t :: %__MODULE__{
          upstream_host: :inet.ip_address(),
          upstream_port: :inet.port_number(),
          listen_ip: :inet.ip_address(),
          listen_port: :inet.port_number(),
          impairment: %{up: Impairment.t(), down: Impairment.t()},
          seed: integer(),
          history: ChaosProxy.limit(pos_integer()),
          client_idle_ms: ChaosProxy.limit(non_neg_integer()),
          max_clients: ChaosProxy.limit(pos_integer())
        }

  @enforce_keys [
    :upstream_host,
    :upstream_port,
    :listen_ip,
    :listen_port,
    :impairment,
    :seed,
    :history,
    :client_idle_ms,
    :max_clients
  ]
  defstruct @enforce_keys

  @defaults [
    upstream_host: "127.0.0.1",
    listen_ip: {127, 0, 0, 1},
    listen_port: 0,
    impairment: [],
    seed: 0,
    history: 300,
    client_idle_ms: 60_000,
    max_clients: :infinity
  ]

  @expected [
    upstream_port: "a port",
    listen_ip: "an IP address",
    listen_port: "a port or 0",
    seed: "an integer",
    history: "a positive integer or :infinity",
    client_idle_ms: "a non-negative integer or :infinity",
    max_clients: "a positive integer or :infinity"
  ]

  @spec parse(ChaosProxy.options()) :: {:ok, t()} | {:error, String.t()}
  def parse(opts) do
    with {:ok, opts} <- known(opts),
         :ok <- check(opts),
         {:ok, impairment} <- Impairment.parse(opts[:impairment]),
         {:ok, upstream_host} <- resolve(opts[:upstream_host]) do
      transparent = %{up: %Impairment{}, down: %Impairment{}}

      {:ok,
       %__MODULE__{
         upstream_host: upstream_host,
         upstream_port: opts[:upstream_port],
         listen_ip: opts[:listen_ip],
         listen_port: opts[:listen_port],
         impairment: Map.merge(transparent, impairment),
         seed: opts[:seed],
         history: opts[:history],
         client_idle_ms: opts[:client_idle_ms],
         max_clients: opts[:max_clients]
       }}
    end
  end

  @spec known(term()) :: {:ok, keyword()} | {:error, String.t()}
  defp known(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, "options must be a keyword list, got: #{inspect(opts)}"}

      not Keyword.has_key?(opts, :upstream_port) ->
        {:error, ":upstream_port is required"}

      true ->
        with {:error, keys} <- Keyword.validate(opts, [:upstream_port, :name | @defaults]),
             do: {:error, "unknown keys #{inspect(keys)}"}
    end
  end

  @spec check(keyword()) :: :ok | {:error, String.t()}
  defp check(opts) do
    case Enum.find(@expected, fn {key, _expected} -> not valid?(key, opts[key]) end) do
      nil ->
        :ok

      {key, expected} ->
        {:error, "#{inspect(key)} must be #{expected}, got: #{inspect(opts[key])}"}
    end
  end

  @spec valid?(atom(), term()) :: boolean()
  defp valid?(:upstream_port, port), do: port in 1..65_535
  defp valid?(:listen_ip, ip), do: :inet.is_ip_address(ip)
  defp valid?(:listen_port, port), do: port in 0..65_535
  defp valid?(:seed, seed), do: is_integer(seed)
  defp valid?(:history, history), do: limit?(history, 1)
  defp valid?(:client_idle_ms, idle_ms), do: limit?(idle_ms, 0)
  defp valid?(:max_clients, max_clients), do: limit?(max_clients, 1)

  @spec limit?(term(), non_neg_integer()) :: boolean()
  defp limit?(value, min), do: value == :infinity or (is_integer(value) and value >= min)

  @spec resolve(term()) :: {:ok, :inet.ip_address()} | {:error, String.t()}
  defp resolve(host) do
    name = if is_binary(host), do: String.to_charlist(host), else: host

    with {:error, _v4} <- :inet.getaddr(name, :inet),
         {:error, reason} <- :inet.getaddr(name, :inet6) do
      {:error, "cannot resolve :upstream_host #{inspect(host)}: #{inspect(reason)}"}
    end
  end
end
