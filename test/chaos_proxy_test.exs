defmodule ChaosProxyTest do
  use ExUnit.Case, async: true

  alias ChaosProxy.Impairment

  # Large enough for a burst released on one timer tick; the default is small
  # on macOS.
  @socket_opts [:binary, active: true, ip: {127, 0, 0, 1}, recbuf: 1_000_000]

  # A stand-in upstream: echoes every datagram back to its sender.
  defp echo_server do
    {:ok, socket} = :gen_udp.open(0, @socket_opts)
    {:ok, port} = :inet.port(socket)
    parent = self()

    pid =
      spawn_link(fn ->
        Stream.repeatedly(fn ->
          receive do
            {:udp, ^socket, ip, from, data} ->
              :gen_udp.send(socket, ip, from, data)
              send(parent, {:echoed, data})
          end
        end)
        |> Stream.run()
      end)

    :ok = :gen_udp.controlling_process(socket, pid)
    port
  end

  defp client do
    {:ok, socket} = :gen_udp.open(0, @socket_opts)
    socket
  end

  defp start(opts \\ []) do
    {:ok, proxy} = ChaosProxy.start_link([upstream_port: echo_server(), seed: 1] ++ opts)
    {proxy, ChaosProxy.port(proxy)}
  end

  test "forwards both ways and counts bytes per direction" do
    {proxy, port} = start()
    socket = client()
    :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "hello")
    assert_receive {:udp, ^socket, _, ^port, "hello"}, 1_000

    assert %{
             totals: %{
               up: %{offered_bytes: 5, forwarded_bytes: 5},
               down: %{offered_bytes: 5, forwarded_bytes: 5, dropped_packets: 0}
             },
             clients: 1
           } = ChaosProxy.report(proxy)
  end

  test "a bare impairment shapes the downlink and mirrors its delay" do
    {proxy, port} = start()
    :ok = ChaosProxy.apply(proxy, %Impairment{rate_kbps: 80, queue_ms: 10, delay_ms: 100})
    socket = client()

    started = System.monotonic_time(:millisecond)
    :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "ping")
    assert_receive {:udp, ^socket, _, ^port, "ping"}, 2_000
    assert System.monotonic_time(:millisecond) - started >= 180

    # 80 kbit/s with the 8-packet queue floor: a burst of 20 loses some on the
    # way back only (how many depends on how spread out the echoes arrive;
    # the exact arithmetic is in ChaosProxy.LinkTest).
    packet = :binary.copy(<<1>>, 1_500)
    for _i <- 1..20, do: :gen_udp.send(socket, {127, 0, 0, 1}, port, packet)
    for _i <- 1..20, do: assert_receive({:echoed, _data}, 1_000)
    # The echoes reach the proxy around when they reach this process.
    Process.sleep(100)

    %{totals: %{up: up, down: down}} = ChaosProxy.report(proxy)
    assert up.dropped_packets == 0
    assert down.dropped_packets > 0
  end

  test "shapes the uplink on its own" do
    {proxy, port} = start(impairment: [up: %Impairment{rate_kbps: 80, queue_ms: 10}])
    socket = client()
    packet = :binary.copy(<<1>>, 1_500)
    for _i <- 1..20, do: :gen_udp.send(socket, {127, 0, 0, 1}, port, packet)
    Process.sleep(300)

    %{totals: %{up: up, down: down}} = ChaosProxy.report(proxy)
    assert up.dropped_packets >= 9
    assert down.dropped_packets == 0
    assert up.forwarded_bytes < 20 * 1_500
  end

  test "a blackout drops everything until lifted" do
    {proxy, port} = start(impairment: %Impairment{blackout?: true})
    socket = client()
    :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "lost")
    refute_receive {:udp, ^socket, _, _, _}, 200
    assert %{totals: %{up: %{blackout_packets: 1}}} = ChaosProxy.report(proxy)

    :ok = ChaosProxy.apply(proxy, %Impairment{})
    :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "back")
    assert_receive {:udp, ^socket, _, ^port, "back"}, 1_000
  end

  test "reaches an IPv6 upstream" do
    {:ok, upstream} =
      :gen_udp.open(0, [:binary, :inet6, active: true, ip: {0, 0, 0, 0, 0, 0, 0, 1}])

    {:ok, upstream_port} = :inet.port(upstream)
    {:ok, proxy} = ChaosProxy.start_link(upstream_host: "::1", upstream_port: upstream_port)
    port = ChaosProxy.port(proxy)

    :ok = :gen_udp.send(client(), {127, 0, 0, 1}, port, "v6")
    assert_receive {:udp, ^upstream, _, _, "v6"}, 1_000
  end

  test "idle clients are forgotten" do
    {proxy, port} = start(client_idle_ms: 100)
    socket = client()
    :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "hello")
    assert_receive {:udp, ^socket, _, ^port, "hello"}, 1_000
    assert %{clients: 1} = ChaosProxy.report(proxy)

    Process.sleep(200)
    assert %{clients: 0} = ChaosProxy.report(proxy)

    # A returning client gets a fresh upstream socket.
    :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "again")
    assert_receive {:udp, ^socket, _, ^port, "again"}, 1_000
  end

  test "history bounds the per-second buckets but not the totals" do
    {proxy, port} = start(history: 2)
    socket = client()

    for _i <- 1..3 do
      :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "12345")
      assert_receive {:udp, ^socket, _, ^port, "12345"}, 1_000
      Process.sleep(1_000)
    end

    assert %{seconds: seconds, totals: %{down: %{forwarded_bytes: 15}}} =
             ChaosProxy.report(proxy)

    assert length(seconds) <= 2
    assert [%{second: _second, up: _up, down: _down} | _rest] = seconds
  end
end
