defmodule ChaosProxy.Options do
  @moduledoc false
  # A `%ChaosProxy{}` parsed in the proxy's `init/1`: every field checked, the
  # upstream resolved to an address and the impairment split per direction.
  # The proxy trusts what it holds.

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

  # What the fields are checked against, in this order; `:upstream_host` and
  # `:impairment` are parsed on their own.
  @expected [
    upstream_port: "a port",
    listen_ip: "an IP address",
    listen_port: "a port or 0",
    seed: "an integer",
    history: "a positive integer or :infinity",
    client_idle_ms: "a non-negative integer or :infinity",
    max_clients: "a positive integer or :infinity"
  ]

  @spec parse(ChaosProxy.t()) :: {:ok, t()} | {:error, String.t()}
  def parse(%ChaosProxy{} = proxy) do
    with :ok <- check(proxy),
         {:ok, impairment} <- Impairment.parse(proxy.impairment),
         {:ok, upstream_host} <- resolve(proxy.upstream_host) do
      transparent = %{up: %Impairment{}, down: %Impairment{}}

      {:ok,
       %__MODULE__{
         upstream_host: upstream_host,
         upstream_port: proxy.upstream_port,
         listen_ip: proxy.listen_ip,
         listen_port: proxy.listen_port,
         impairment: Map.merge(transparent, impairment),
         seed: proxy.seed,
         history: proxy.history,
         client_idle_ms: proxy.client_idle_ms,
         max_clients: proxy.max_clients
       }}
    end
  end

  @spec check(ChaosProxy.t()) :: :ok | {:error, String.t()}
  defp check(proxy) do
    case Enum.find(@expected, fn {key, _expected} -> not valid?(key, Map.fetch!(proxy, key)) end) do
      nil ->
        :ok

      {key, expected} ->
        {:error, "#{inspect(key)} must be #{expected}, got: #{inspect(Map.fetch!(proxy, key))}"}
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

  # Either family; an address passes through as it is.
  @spec resolve(term()) :: {:ok, :inet.ip_address()} | {:error, String.t()}
  defp resolve(host) do
    name = if is_binary(host), do: String.to_charlist(host), else: host

    with {:error, _v4} <- :inet.getaddr(name, :inet),
         {:error, reason} <- :inet.getaddr(name, :inet6) do
      {:error, "cannot resolve :upstream_host #{inspect(host)}: #{inspect(reason)}"}
    end
  end
end
