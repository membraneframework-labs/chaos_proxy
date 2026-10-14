defmodule ChaosProxy.OptionsTest do
  use ExUnit.Case, async: true

  alias ChaosProxy.{Impairment, Options}

  @spec parse!(keyword()) :: Options.t()
  defp parse!(fields) do
    {:ok, options} = Options.parse(struct!(ChaosProxy, [upstream_port: 1] ++ fields))
    options
  end

  test "options that fail validation stop the proxy as it starts" do
    for {fields, message} <- [
          {[upstream_port: nil], ~r/:upstream_port must be a port, got: nil/},
          {[upstream_port: 65_536], ~r/:upstream_port must be a port/},
          {[listen_ip: "127.0.0.1"], ~r/:listen_ip must be an IP address/},
          {[listen_port: -1], ~r/:listen_port must be a port or 0/},
          {[seed: 1.5], ~r/:seed must be an integer/},
          {[history: 0], ~r/:history must be a positive integer or :infinity/},
          {[client_idle_ms: -1], ~r/:client_idle_ms must be a non-negative integer/},
          {[max_clients: 0], ~r/:max_clients must be a positive integer or :infinity/},
          {[impairment: [up: %Impairment{rate_kbps: 0}]], ~r/:rate_kbps must be above 0/},
          {[upstream_host: "no such host.invalid"], ~r/cannot resolve :upstream_host/}
        ] do
      proxy = struct!(ChaosProxy, [upstream_port: 1] ++ fields)
      assert {:error, {:validation_failed, reason}} = ChaosProxy.start(proxy)
      assert reason =~ message
    end
  end

  test "resolves the upstream host to an address" do
    assert parse!(upstream_host: "::1").upstream_host == {0, 0, 0, 0, 0, 0, 0, 1}
    assert parse!(upstream_host: ~c"127.0.0.1").upstream_host == {127, 0, 0, 1}
    assert parse!(upstream_host: {10, 0, 0, 1}).upstream_host == {10, 0, 0, 1}
  end

  test "leaves a direction the impairment does not name transparent" do
    thin = %Impairment{rate_kbps: 500, delay_ms: 10}
    assert parse!(impairment: [up: thin]).impairment == %{up: thin, down: %Impairment{}}
  end
end
