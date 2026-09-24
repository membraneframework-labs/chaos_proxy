defmodule ChaosProxyTest do
  use ExUnit.Case, async: true

  alias ChaosProxy, as: Proxy
  alias ChaosProxy.Impairment

  # A stand-in relay: echoes every datagram back to its sender.
  defp echo_server do
    {:ok, socket} = :gen_udp.open(0, [:binary, active: true, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    parent = self()

    pid =
      spawn_link(fn ->
        :gen_udp.controlling_process(socket, self())

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
    {:ok, socket} = :gen_udp.open(0, [:binary, active: true, ip: {127, 0, 0, 1}])
    socket
  end

  defp start(impairment, seed \\ 1, opts \\ []) do
    relay_port = echo_server()

    {:ok, proxy} =
      Proxy.start_link([relay_port: relay_port, seed: seed, impairment: impairment] ++ opts)

    {proxy, Proxy.port(proxy)}
  end

  test "forwards both ways and counts bytes" do
    {proxy, port} = start(Impairment.clean(0))
    socket = client()
    :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "hello")
    assert_receive {:udp, ^socket, _, ^port, "hello"}, 1_000

    assert %{totals: %{offered_bytes: 5, forwarded_bytes: 5, dropped_packets: 0}} =
             Proxy.report(proxy)
  end

  test "delays both directions" do
    {_proxy, port} = start(%Impairment{rate_kbps: 8_000, delay_ms: 100})
    socket = client()
    started = System.monotonic_time(:millisecond)
    :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "ping")
    assert_receive {:udp, ^socket, _, ^port, "ping"}, 2_000
    assert System.monotonic_time(:millisecond) - started >= 180
  end

  test "a blackout drops everything until lifted" do
    {proxy, port} = start(%Impairment{blackout?: true})
    socket = client()
    :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "lost")
    refute_receive {:udp, ^socket, _, _, _}, 200
    assert %{totals: %{blackout_packets: 1}} = Proxy.report(proxy)

    :ok = Proxy.apply(proxy, Impairment.clean(0))
    :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "back")
    assert_receive {:udp, ^socket, _, ^port, "back"}, 1_000
  end

  test "random loss is seeded" do
    losses =
      for seed <- [7, 7, 8] do
        {proxy, port} = start(%Impairment{rate_kbps: 8_000, delay_ms: 0, loss_pct: 50.0}, seed)
        socket = client()
        for i <- 1..40, do: :gen_udp.send(socket, {127, 0, 0, 1}, port, "p#{i}")
        for _ <- 1..40, do: assert_receive({:echoed, _}, 1_000)
        Process.sleep(50)
        %{totals: %{lost_packets: lost}} = Proxy.report(proxy)
        GenServer.stop(proxy)
        lost
      end

    assert [a, a, b] = losses
    assert a > 5 and a < 35
    assert b != a
  end

  test "the token bucket paces and the queue tail-drops" do
    # 80 kbit/s = 10 bytes/ms; 1500-byte packets take 150 ms each; the queue
    # holds 8 packets (the floor), so a burst of 20 loses at least 9.
    {proxy, port} = start(%Impairment{rate_kbps: 80, delay_ms: 0, queue_ms: 10})
    socket = client()
    packet = :binary.copy(<<1>>, 1_500)
    for _ <- 1..20, do: :gen_udp.send(socket, {127, 0, 0, 1}, port, packet)
    for _ <- 1..20, do: assert_receive({:echoed, _}, 1_000)
    Process.sleep(500)
    %{totals: totals} = Proxy.report(proxy)
    assert totals.dropped_packets >= 9
    assert totals.forwarded_bytes < 20 * 1_500
    assert totals.forwarded_bytes > 0
  end

  test "idle clients are forgotten" do
    {proxy, port} = start(Impairment.clean(0), 1, client_idle_ms: 100)
    socket = client()
    :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "hello")
    assert_receive {:udp, ^socket, _, ^port, "hello"}, 1_000
    assert %{clients: 1} = Proxy.report(proxy)

    Process.sleep(200)
    assert %{clients: 0} = Proxy.report(proxy)

    # A returning client gets a fresh upstream socket.
    :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "again")
    assert_receive {:udp, ^socket, _, ^port, "again"}, 1_000
  end

  test "history bounds the per-second buckets but not the totals" do
    {proxy, port} = start(%Impairment{rate_kbps: 8_000, delay_ms: 0}, 1, history: 2)
    socket = client()

    for _ <- 1..3 do
      :ok = :gen_udp.send(socket, {127, 0, 0, 1}, port, "12345")
      assert_receive {:udp, ^socket, _, ^port, "12345"}, 1_000
      Process.sleep(1_000)
    end

    assert %{seconds: seconds, totals: %{forwarded_bytes: 15}} = Proxy.report(proxy)
    assert length(seconds) <= 2
  end
end
