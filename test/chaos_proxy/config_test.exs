defmodule ChaosProxy.ConfigTest do
  use ExUnit.Case, async: true

  alias ChaosProxy.{Config, Impairment}

  test "resolves the upstream host to an address" do
    assert Config.new!(upstream_host: "::1", upstream_port: 1).upstream_host ==
             {0, 0, 0, 0, 0, 0, 0, 1}

    assert Config.new!(upstream_host: ~c"127.0.0.1", upstream_port: 1).upstream_host ==
             {127, 0, 0, 1}

    assert Config.new!(upstream_host: {10, 0, 0, 1}, upstream_port: 1).upstream_host ==
             {10, 0, 0, 1}
  end

  test "has an impairment for each direction, whichever form apply/2 takes was given" do
    thin = %Impairment{rate_kbps: 500, delay_ms: 10}

    assert Config.new!(upstream_port: 1, impairment: thin).impairment ==
             %{down: thin, up: %Impairment{delay_ms: 10}}

    assert Config.new!(upstream_port: 1, impairment: [up: thin]).impairment ==
             %{up: thin, down: %Impairment{}}
  end

  test "refuses an unknown option" do
    assert_raise KeyError, ~r/max_client/, fn -> Config.new!(upstream_port: 1, max_client: 3) end
  end

  test "refuses a host that does not resolve" do
    assert_raise ArgumentError, ~r/cannot resolve upstream/, fn ->
      Config.new!(upstream_host: "no such host.invalid", upstream_port: 1)
    end
  end

  test "refuses a history that keeps no second at all" do
    assert_raise ArgumentError, ~r/:history must be/, fn ->
      Config.new!(upstream_port: 1, history: 0)
    end
  end
end
