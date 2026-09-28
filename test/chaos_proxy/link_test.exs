defmodule ChaosProxy.LinkTest do
  use ExUnit.Case, async: true

  alias ChaosProxy.{Impairment, Link}

  @packet 1_500

  # Offers `n` full-size packets at `now` and returns the outcomes.
  defp burst(link, n, now) do
    Enum.map_reduce(1..n, link, fn i, link -> Link.offer(link, i, @packet, now) end)
  end

  test "a transparent link releases everything at once" do
    link = Link.new(%Impairment{}, 0, 0)
    {outcomes, link} = burst(link, 100, 0)
    assert Enum.all?(outcomes, &(&1 == :queued))
    assert {released, _link, nil} = Link.drain(link, 0)
    assert released == Enum.to_list(1..100)
  end

  test "the bucket paces at the rate and says when to come back" do
    # 120 kbit/s = 15 bytes/ms: a 1500-byte packet every 100 ms, after a
    # burst allowance of 3 packets once the bucket has filled.
    link = Link.new(%Impairment{rate_kbps: 120, queue_ms: 10_000}, 0, 0)
    {_outcomes, link} = burst(link, 5, 0)

    assert {[], link, 100} = Link.drain(link, 0)
    assert {[1], link, 100} = Link.drain(link, 100)
    assert {[2], link, 50} = Link.drain(link, 250)
    assert {[3, 4, 5], link, nil} = Link.drain(link, 1_000)
    assert Link.queue_bytes(link) == 0
  end

  test "the queue tail-drops beyond queue_ms, never below 8 packets" do
    # 120 kbit/s for 100 ms is 1500 bytes: the floor of 8 packets applies.
    link = Link.new(%Impairment{rate_kbps: 120, queue_ms: 100}, 0, 0)
    {outcomes, link} = burst(link, 10, 0)
    assert Enum.frequencies(outcomes) == %{queued: 8, dropped: 2}
    assert Link.queue_bytes(link) == 8 * @packet
  end

  test "loss is seeded" do
    losses = fn seed ->
      link = Link.new(%Impairment{loss_pct: 30.0}, seed, 0)
      {outcomes, _link} = burst(link, 200, 0)
      Enum.count(outcomes, &(&1 == :lost))
    end

    assert losses.(7) == losses.(7)
    assert losses.(7) != losses.(8)
    assert losses.(7) in 30..90
  end

  test "a blackout refuses packets but lets the queue drain" do
    link = Link.new(%Impairment{rate_kbps: 120, queue_ms: 10_000}, 0, 0)
    {_outcomes, link} = burst(link, 2, 0)
    link = Link.apply(link, %Impairment{rate_kbps: 120, queue_ms: 10_000, blackout?: true}, 0)

    assert {:blackout, link} = Link.offer(link, :late, @packet, 0)
    assert {[1, 2], _link, nil} = Link.drain(link, 1_000)
  end

  test "a new rate applies to what is already queued" do
    link = Link.new(%Impairment{rate_kbps: 120, queue_ms: 10_000}, 0, 0)
    {_outcomes, link} = burst(link, 3, 0)
    link = Link.apply(link, %Impairment{}, 10)
    assert {[1, 2, 3], _link, nil} = Link.drain(link, 10)
  end
end
