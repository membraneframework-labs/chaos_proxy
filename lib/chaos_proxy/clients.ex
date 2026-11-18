defmodule ChaosProxy.Clients do
  @moduledoc false
  # Who is behind a `ChaosProxy`, as plain data: each client with its upstream
  # socket and when it was last heard of, in either direction. Time is passed
  # in; sockets are only held, closing an expired one is up to the caller.

  @type client :: {:inet.ip_address(), :inet.port_number()}
  @type socket :: term()

  @type t :: %__MODULE__{
          sockets: %{client() => %{socket: socket(), seen_ms: number()}},
          clients: %{socket() => client()}
        }

  defstruct sockets: %{}, clients: %{}

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{sockets: sockets}), do: map_size(sockets)

  @spec socket(t(), client()) :: {:ok, socket()} | :error
  def socket(%__MODULE__{sockets: sockets}, client) do
    with {:ok, %{socket: socket}} <- Map.fetch(sockets, client), do: {:ok, socket}
  end

  @spec client(t(), socket()) :: {:ok, client()} | :error
  def client(%__MODULE__{clients: clients}, socket), do: Map.fetch(clients, socket)

  @doc "Adds a client, or notes that a known one was heard of at `now`."
  @spec put(t(), client(), socket(), number()) :: t()
  def put(%__MODULE__{} = clients, client, socket, now) do
    %__MODULE__{
      sockets: Map.put(clients.sockets, client, %{socket: socket, seen_ms: now}),
      clients: Map.put(clients.clients, socket, client)
    }
  end

  @doc "Forgets the clients not heard of for `idle_ms` and returns their sockets."
  @spec expire(t(), non_neg_integer() | :infinity, number()) :: {[socket()], t()}
  def expire(%__MODULE__{} = clients, :infinity, _now), do: {[], clients}

  def expire(%__MODULE__{} = clients, idle_ms, now) do
    {idle, live} =
      Map.split_with(clients.sockets, fn {_client, %{seen_ms: seen}} -> seen < now - idle_ms end)

    sockets = Enum.map(idle, fn {_client, %{socket: socket}} -> socket end)
    {sockets, %__MODULE__{sockets: live, clients: Map.drop(clients.clients, sockets)}}
  end
end
