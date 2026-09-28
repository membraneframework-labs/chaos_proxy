defmodule ChaosProxy.ConfigTest do
  use ExUnit.Case, async: true

  alias ChaosProxy.{Config, Impairment}

  test "defaults to a transparent proxy on loopback, on a port the OS picks" do
    assert %Config{
             upstream_host: {127, 0, 0, 1},
             upstream_port: 4443,
             listen_ip: {127, 0, 0, 1},
             listen_port: 0,
             impairment: [],
             seed: 0,
             history: 300,
             client_idle_ms: 60_000,
             max_clients: :infinity,
             name: nil
           } = Config.new!(upstream_port: 4443)
  end

  test "resolves the upstream host to an address" do
    assert Config.new!(upstream_host: "::1", upstream_port: 1).upstream_host ==
             {0, 0, 0, 0, 0, 0, 0, 1}

    assert Config.new!(upstream_host: ~c"127.0.0.1", upstream_port: 1).upstream_host ==
             {127, 0, 0, 1}

    assert Config.new!(upstream_host: {10, 0, 0, 1}, upstream_port: 1).upstream_host ==
             {10, 0, 0, 1}
  end

  test "takes an impairment in every form apply/2 does" do
    down = %Impairment{rate_kbps: 500}

    assert Config.new!(upstream_port: 1, impairment: down).impairment == down
    assert Config.new!(upstream_port: 1, impairment: [up: down]).impairment == [up: down]
  end

  test "requires the upstream port" do
    assert_raise ArgumentError, ~r/upstream_port/, fn -> Config.new!([]) end
  end

  test "refuses an unknown option" do
    assert_raise KeyError, ~r/max_client/, fn -> Config.new!(upstream_port: 1, max_client: 3) end
  end

  test "refuses a host that does not resolve" do
    assert_raise ArgumentError, ~r/cannot resolve upstream/, fn ->
      Config.new!(upstream_host: "no such host.invalid", upstream_port: 1)
    end
  end

  for {key, value} <- [
        upstream_port: 0,
        upstream_port: 65_536,
        upstream_host: 4443,
        upstream_host: {1, 2, 3},
        listen_port: -1,
        listen_ip: "127.0.0.1",
        impairment: [sideways: %Impairment{}],
        impairment: [up: :slow],
        seed: 1.5,
        history: 0,
        client_idle_ms: -1,
        max_clients: 0
      ] do
    test "refuses #{key}: #{inspect(value)}" do
      opts = Keyword.merge([upstream_port: 1], [{unquote(key), unquote(Macro.escape(value))}])
      assert_raise ArgumentError, ~r/#{unquote(key)} must be/, fn -> Config.new!(opts) end
    end
  end
end
