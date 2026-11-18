defmodule ChaosProxy.ClientsTest do
  use ExUnit.Case, async: true

  alias ChaosProxy.Clients

  @alice {{127, 0, 0, 1}, 50_001}
  @bob {{127, 0, 0, 1}, 50_002}

  test "finds a client by its socket and a socket by its client" do
    clients = Clients.new() |> Clients.put(@alice, :a, 0) |> Clients.put(@bob, :b, 0)

    assert Clients.size(clients) == 2
    assert Clients.socket(clients, @alice) == {:ok, :a}
    assert Clients.client(clients, :b) == {:ok, @bob}
  end

  test "knows nobody at first" do
    assert Clients.size(Clients.new()) == 0
    assert Clients.socket(Clients.new(), @alice) == :error
    assert Clients.client(Clients.new(), :a) == :error
  end

  test "a client heard of again is the same client" do
    clients = Clients.new() |> Clients.put(@alice, :a, 0) |> Clients.put(@alice, :a, 10)

    assert Clients.size(clients) == 1
  end

  test "forgets the clients silent for longer than idle_ms and returns their sockets" do
    clients = Clients.new() |> Clients.put(@alice, :a, 0) |> Clients.put(@bob, :b, 50)

    assert {[], ^clients} = Clients.expire(clients, 100, 100)
    assert {[:a], clients} = Clients.expire(clients, 100, 101)

    assert Clients.size(clients) == 1
    assert Clients.socket(clients, @alice) == :error
    assert Clients.client(clients, :a) == :error
    assert Clients.socket(clients, @bob) == {:ok, :b}
  end

  test "being heard of puts expiry off" do
    clients = Clients.new() |> Clients.put(@alice, :a, 0) |> Clients.put(@alice, :a, 90)

    assert {[], ^clients} = Clients.expire(clients, 100, 150)
    assert {[:a], _clients} = Clients.expire(clients, 100, 191)
  end

  test "forgets nobody when clients never go idle" do
    clients = Clients.new() |> Clients.put(@alice, :a, 0)

    assert {[], ^clients} = Clients.expire(clients, :infinity, 1_000_000_000)
  end
end
