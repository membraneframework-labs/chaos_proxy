defmodule ChaosProxyTest do
  # The shaping, the counters and the client table are tested as plain data
  # (ChaosProxy.LinkTest, StatsTest, ClientsTest). These tests are about the
  # sockets around them and assert only what holds however the packets and
  # the timers are spaced.
  use ExUnit.Case, async: true

  alias ChaosProxy.{Config, Impairment}

  @loopback {127, 0, 0, 1}
  @socket_opts [:binary, active: true, ip: @loopback, recbuf: 1_000_000]
  @packet :binary.copy(<<1>>, 1_500)

  # A stand-in upstream: echoes every datagram back to its sender.
  defp echo_server do
    {:ok, socket} = :gen_udp.open(0, @socket_opts)
    {:ok, port} = :inet.port(socket)

    pid = spawn_link(fn -> echo(socket) end)
    :ok = :gen_udp.controlling_process(socket, pid)
    port
  end

  defp echo(socket) do
    receive do
      {:udp, ^socket, ip, from, data} -> :gen_udp.send(socket, ip, from, data)
    end

    echo(socket)
  end

  defp client do
    {:ok, socket} = :gen_udp.open(0, @socket_opts)
    socket
  end

  defp start(opts \\ []) do
    config = Config.new!([upstream_port: echo_server(), seed: 1] ++ opts)
    proxy = start_supervised!({ChaosProxy, config})
    {proxy, ChaosProxy.port(proxy)}
  end

  defp send_to(socket, port, data), do: :ok = :gen_udp.send(socket, @loopback, port, data)

  # The proxy's report once it matches `pattern`: the proxy learns of a packet
  # some time after it was sent.
  defmacrop report_matching(proxy, pattern) do
    quote do
      deadline = System.monotonic_time(:millisecond) + 5_000

      Stream.repeatedly(fn -> ChaosProxy.report(unquote(proxy)) end)
      |> Enum.find(fn report ->
        match?(unquote(pattern), report) or
          (System.monotonic_time(:millisecond) > deadline and
             flunk("the report never matched, last: #{inspect(report)}"))
      end)
    end
  end

  test "forwards both ways and counts bytes per direction" do
    {proxy, port} = start()
    socket = client()

    send_to(socket, port, "hello")
    assert_receive {:udp, ^socket, @loopback, ^port, "hello"}, 5_000

    assert %{
             totals: %{up: up, down: down},
             seconds: [%{second: _second, up: _up, down: _down} | _rest],
             clients: 1
           } = ChaosProxy.report(proxy)

    assert up == down
    assert %{offered_bytes: 5, forwarded_bytes: 5} = up

    assert Map.drop(up, [:offered_bytes, :forwarded_bytes]) == %{
             dropped_packets: 0,
             lost_packets: 0,
             blackout_packets: 0,
             queue_bytes_max: 5,
             refused_packets: 0
           }
  end

  test "each client has its own peer at the upstream" do
    {proxy, port} = start()
    first = client()
    second = client()

    send_to(first, port, "first")
    send_to(second, port, "second")

    assert_receive {:udp, ^first, _ip, ^port, "first"}, 5_000
    assert_receive {:udp, ^second, _ip, ^port, "second"}, 5_000
    assert %{clients: 2} = ChaosProxy.report(proxy)
    refute_received {:udp, _socket, _ip, _port, _data}
  end

  test "reaches an IPv6 upstream" do
    {:ok, upstream} =
      :gen_udp.open(0, [:binary, :inet6, active: true, ip: {0, 0, 0, 0, 0, 0, 0, 1}])

    {:ok, upstream_port} = :inet.port(upstream)
    config = Config.new!(upstream_host: "::1", upstream_port: upstream_port)
    proxy = start_supervised!({ChaosProxy, config})

    send_to(client(), ChaosProxy.port(proxy), "v6")
    assert_receive {:udp, ^upstream, _ip, _port, "v6"}, 5_000
  end

  test "registers under the config's name" do
    name = :"proxy_#{System.unique_integer([:positive])}"
    proxy = start_supervised!({ChaosProxy, Config.new!(upstream_port: echo_server(), name: name)})

    assert Process.whereis(name) == proxy
    assert ChaosProxy.port(name) == ChaosProxy.port(proxy)
  end

  test "listens on the port it was given" do
    {:ok, socket} = :gen_udp.open(0, @socket_opts)
    {:ok, free} = :inet.port(socket)
    :ok = :gen_udp.close(socket)

    assert {_proxy, ^free} = start(listen_port: free)
  end

  test "says so when the port it was given is taken" do
    Process.flag(:trap_exit, true)
    {:ok, socket} = :gen_udp.open(0, @socket_opts)
    {:ok, taken} = :inet.port(socket)
    config = Config.new!(upstream_port: echo_server(), listen_port: taken)

    assert ChaosProxy.start_link(config) == {:error, {:listen, :eaddrinuse}}
  end

  test "apply/2 refuses a rate that lets nothing through and keeps the one it had" do
    {proxy, port} = start(impairment: %Impairment{delay_ms: 1})
    socket = client()

    assert_raise ArgumentError, ~r/rate_kbps must be above 0/, fn ->
      ChaosProxy.apply(proxy, %Impairment{rate_kbps: 0.0})
    end

    send_to(socket, port, "still")
    assert_receive {:udp, ^socket, _ip, ^port, "still"}, 5_000
    assert Process.alive?(proxy)
  end

  test "a packet takes no less than the delay of both directions" do
    {_proxy, port} = start(impairment: [up: %Impairment{delay_ms: 30}])
    socket = client()
    sent_at = System.monotonic_time(:millisecond)

    send_to(socket, port, "ping")
    assert_receive {:udp, ^socket, _ip, ^port, "ping"}, 5_000
    assert System.monotonic_time(:millisecond) - sent_at >= 30
  end

  test "a bare impairment is mirrored on the uplink" do
    {proxy, port} = start(impairment: %Impairment{delay_ms: 30})
    socket = client()
    sent_at = System.monotonic_time(:millisecond)

    send_to(socket, port, "ping")
    assert_receive {:udp, ^socket, _ip, ^port, "ping"}, 5_000
    assert System.monotonic_time(:millisecond) - sent_at >= 60

    :ok = ChaosProxy.apply(proxy, %Impairment{blackout?: true})
    send_to(socket, port, "lost")
    report_matching(proxy, %{totals: %{up: %{blackout_packets: 1}}})
  end

  test "a delayed link delivers in order" do
    {_proxy, port} = start(impairment: %Impairment{delay_ms: 20})
    socket = client()
    for i <- 1..500, do: send_to(socket, port, <<i::16>>)

    received =
      for _i <- 1..500 do
        assert_receive {:udp, ^socket, _ip, ^port, <<i::16>>}, 5_000
        i
      end

    assert received == Enum.to_list(1..500)
  end

  test "shapes the uplink on its own" do
    # At 1 kbit/s the bucket takes 12 s to pay for one packet, so none of
    # these leaves: the queue takes its floor of 8 and the rest is dropped.
    {proxy, port} = start(impairment: [up: %Impairment{rate_kbps: 1, queue_ms: 10}])
    socket = client()
    for _i <- 1..20, do: send_to(socket, port, @packet)

    assert %{totals: %{up: up, down: down}} =
             report_matching(proxy, %{totals: %{up: %{offered_bytes: 30_000}}})

    assert %{dropped_packets: 12, forwarded_bytes: 0, queue_bytes_max: 12_000} = up
    assert %{offered_bytes: 0, dropped_packets: 0} = down
  end

  test "shapes the downlink on its own" do
    {proxy, port} = start(impairment: [down: %Impairment{rate_kbps: 1, queue_ms: 10}])
    socket = client()
    for _i <- 1..20, do: send_to(socket, port, @packet)

    assert %{totals: %{up: up, down: down}} =
             report_matching(proxy, %{totals: %{down: %{offered_bytes: 30_000}}})

    assert %{offered_bytes: 30_000, forwarded_bytes: 30_000, dropped_packets: 0} = up
    assert %{dropped_packets: 12, forwarded_bytes: 0, queue_bytes_max: 12_000} = down
  end

  test "what is queued leaves once the rate is raised" do
    {proxy, port} = start(impairment: [up: %Impairment{rate_kbps: 1, queue_ms: 10}])
    socket = client()
    for i <- 1..8, do: send_to(socket, port, <<i::16, 0::size(1_498)-unit(8)>>)
    report_matching(proxy, %{totals: %{up: %{offered_bytes: 12_000}}})

    :ok = ChaosProxy.apply(proxy, up: %Impairment{})

    received =
      for _i <- 1..8 do
        assert_receive {:udp, ^socket, _ip, ^port, <<i::16, _rest::binary>>}, 5_000
        i
      end

    assert received == Enum.to_list(1..8)
  end

  test "apply/2 leaves the direction it does not name as it is" do
    {proxy, port} = start(impairment: [down: %Impairment{blackout?: true}])
    socket = client()

    :ok = ChaosProxy.apply(proxy, up: %Impairment{delay_ms: 1})
    send_to(socket, port, "out")

    assert %{totals: %{up: %{forwarded_bytes: 3, blackout_packets: 0}}} =
             report_matching(proxy, %{totals: %{down: %{blackout_packets: 1}}})

    refute_received {:udp, ^socket, _ip, _port, _data}
  end

  test "a blackout drops everything until lifted" do
    {proxy, port} = start(impairment: %Impairment{blackout?: true})
    socket = client()

    send_to(socket, port, "lost")
    report_matching(proxy, %{totals: %{up: %{offered_bytes: 4, blackout_packets: 1}}})

    :ok = ChaosProxy.apply(proxy, %Impairment{})
    send_to(socket, port, "back")

    assert_receive {:udp, ^socket, _ip, ^port, data}, 5_000
    assert data == "back"
  end

  test "total loss drops everything and says so" do
    {proxy, port} = start(impairment: [up: %Impairment{loss_pct: 100}])
    socket = client()
    for _i <- 1..10, do: send_to(socket, port, "lost")

    assert %{totals: %{up: %{offered_bytes: 40, forwarded_bytes: 0}, down: %{offered_bytes: 0}}} =
             report_matching(proxy, %{totals: %{up: %{lost_packets: 10}}})
  end

  test "idle clients are forgotten, and welcome back" do
    {proxy, port} = start(client_idle_ms: 0)
    socket = client()

    send_to(socket, port, "hello")
    assert_receive {:udp, ^socket, _ip, ^port, "hello"}, 5_000
    report_matching(proxy, %{clients: 0})

    send_to(socket, port, "again")
    assert_receive {:udp, ^socket, _ip, ^port, "again"}, 5_000
  end

  test "refuses clients beyond max_clients while none is idle" do
    {proxy, port} = start(max_clients: 1, client_idle_ms: :infinity)
    first = client()
    second = client()

    send_to(first, port, "first")
    assert_receive {:udp, ^first, _ip, ^port, "first"}, 5_000
    send_to(second, port, "refused")

    assert %{clients: 1, totals: %{up: %{offered_bytes: 5}}} =
             report_matching(proxy, %{totals: %{up: %{refused_packets: 1}}})

    # The one it has is still served.
    send_to(first, port, "still")
    assert_receive {:udp, ^first, _ip, ^port, "still"}, 5_000
    refute_received {:udp, ^second, _ip, _port, _data}
  end

  test "an idle client gives way to a new one at max_clients" do
    {proxy, port} = start(max_clients: 1, client_idle_ms: 0)
    first = client()
    second = client()

    send_to(first, port, "first")
    assert_receive {:udp, ^first, _ip, ^port, "first"}, 5_000

    send_to(second, port, "second")
    assert_receive {:udp, ^second, _ip, ^port, "second"}, 5_000
    assert %{totals: %{up: %{refused_packets: 0}}} = ChaosProxy.report(proxy)
  end
end
