defmodule ChaosProxy.LinkTest do
  use ExUnit.Case, async: true

  alias ChaosProxy.{Impairment, Link}

  @packet 1_500

  # Offers `n` full-size packets, numbered from `first`, and returns the outcomes.
  defp burst(link, n, first \\ 1) do
    Enum.map_reduce(first..(first + n - 1), link, &Link.offer(&2, &1, @packet))
  end

  # The lengths of the runs of lost packets.
  defp runs(outcomes) do
    outcomes
    |> Enum.chunk_by(& &1)
    |> Enum.filter(&(hd(&1) == :lost))
    |> Enum.map(&length/1)
  end

  defp mean(values), do: Enum.sum(values) / length(values)

  defp deviation(values) do
    mean = mean(values)
    :math.sqrt(mean(Enum.map(values, &((&1 - mean) ** 2))))
  end

  # Offers a packet at each of the times and drains whenever the link says
  # to. Returns how long each packet was held, in the order they left in.
  defp held(link, arrivals), do: held(link, Enum.with_index(arrivals), nil, [])

  defp held(_link, [], nil, held), do: Enum.reverse(held)

  defp held(link, [{at, index} | arrivals], wake, held) when wake == nil or at <= wake do
    {:queued, link} = Link.offer(link, {index, at}, @packet)
    drained(link, at, arrivals, held)
  end

  defp held(link, arrivals, wake, held), do: drained(link, wake, arrivals, held)

  defp drained(link, now, arrivals, held) do
    {left, link, wait_ms} = Link.drain(link, now)
    left = for {index, at} <- left, do: {index, now - at}
    held(link, arrivals, wait_ms && now + wait_ms, Enum.reverse(left, held))
  end

  describe "a transparent link" do
    test "lets go of everything at once, in order" do
      link = Link.new(%Impairment{}, 0, 0)
      {outcomes, link} = burst(link, 100)

      assert Enum.all?(outcomes, &(&1 == :queued))
      assert {released, link, nil} = Link.drain(link, 0)
      assert released == Enum.to_list(1..100)
      assert Link.queue_bytes(link) == 0
    end

    test "holds nothing to come back for" do
      assert {[], _link, nil} = Link.drain(Link.new(%Impairment{}, 0, 0), 50)
    end
  end

  describe "the bucket" do
    test "paces at the rate and says when to come back" do
      # 120 kbit/s = 15 bytes/ms: a 1500-byte packet every 100 ms, after a
      # burst allowance of 3 packets once the bucket has filled.
      link = Link.new(%Impairment{rate_kbps: 120, queue_ms: 10_000}, 0, 0)
      {_outcomes, link} = burst(link, 5)

      assert {[], link, 100} = Link.drain(link, 0)
      assert {[1], link, 100} = Link.drain(link, 100)
      assert {[2], link, 50} = Link.drain(link, 250)
      assert {[3, 4, 5], link, nil} = Link.drain(link, 1_000)
      assert Link.queue_bytes(link) == 0
    end

    test "never holds less than three packets, whatever the rate" do
      # 8 kbit/s is 20 bytes in 20 ms; the floor of 3 packets applies.
      link = Link.new(%Impairment{rate_kbps: 8, queue_ms: 100_000}, 0, 0)
      {_outcomes, link} = burst(link, 4)

      assert {[1, 2, 3], _link, 1_500} = Link.drain(link, 1_000_000)
    end

    test "is drained at the new rate once the rate changes" do
      link = Link.new(%Impairment{rate_kbps: 120, queue_ms: 10_000}, 0, 0)
      {_outcomes, link} = burst(link, 3)
      assert {[], link, 100} = Link.drain(link, 0)

      link = Link.apply(link, %Impairment{rate_kbps: 1_200, queue_ms: 10_000}, 0)
      assert {[], link, 10} = Link.drain(link, 0)

      link = Link.apply(link, %Impairment{}, 5)
      assert {[1, 2, 3], _link, nil} = Link.drain(link, 5)
    end

    test "keeps no more than the new burst when the rate drops" do
      # 2400 kbit/s holds 6000 bytes, 120 kbit/s the floor of 4500.
      link = Link.new(%Impairment{rate_kbps: 2_400, queue_ms: 10_000}, 0, 0)
      link = Link.apply(link, %Impairment{rate_kbps: 120, queue_ms: 10_000}, 1_000)
      {_outcomes, link} = burst(link, 4)

      assert {[1, 2, 3], _link, 100} = Link.drain(link, 1_000)
    end
  end

  describe "the queue" do
    test "tail-drops beyond queue_ms" do
      # 1200 kbit/s for 200 ms is 30 000 bytes: 20 packets.
      link = Link.new(%Impairment{rate_kbps: 1_200, queue_ms: 200}, 0, 0)
      {outcomes, link} = burst(link, 25)

      assert Enum.frequencies(outcomes) == %{queued: 20, dropped: 5}
      assert List.last(outcomes) == :dropped
      assert Link.queue_bytes(link) == 20 * @packet
    end

    test "never holds less than eight packets" do
      # 120 kbit/s for 100 ms is 1500 bytes: the floor of 8 packets applies.
      link = Link.new(%Impairment{rate_kbps: 120, queue_ms: 100}, 0, 0)
      {outcomes, link} = burst(link, 10)

      assert Enum.frequencies(outcomes) == %{queued: 8, dropped: 2}
      assert Link.queue_bytes(link) == 8 * @packet
    end

    test "takes packets again once it has drained" do
      link = Link.new(%Impairment{rate_kbps: 120, queue_ms: 100}, 0, 0)
      {_outcomes, link} = burst(link, 8)
      assert {:dropped, link} = Link.offer(link, 9, @packet)

      assert {[1, 2, 3], link, _wait_ms} = Link.drain(link, 1_000)
      assert {:queued, link} = Link.offer(link, 10, @packet)
      assert Link.queue_bytes(link) == 6 * @packet
    end
  end

  describe "loss" do
    test "repeats for a seed and differs between seeds" do
      outcomes = fn seed ->
        link = Link.new(%Impairment{loss_pct: 30}, seed, 0)
        link |> burst(200) |> elem(0)
      end

      assert outcomes.(7) == outcomes.(7)
      assert outcomes.(7) != outcomes.(8)
      assert Enum.sort(Enum.uniq(outcomes.(7))) == [:lost, :queued]
    end

    test "takes nothing at 0 and everything at 100 percent" do
      none = Link.new(%Impairment{loss_pct: 0}, 1, 0)
      all = Link.new(%Impairment{loss_pct: 100}, 1, 0)

      assert {outcomes, _link} = burst(none, 100)
      assert Enum.uniq(outcomes) == [:queued]
      assert {outcomes, _link} = burst(all, 100)
      assert Enum.uniq(outcomes) == [:lost]
    end

    test "does not queue what it took" do
      {_outcomes, link} = burst(Link.new(%Impairment{loss_pct: 100}, 1, 0), 10)

      assert Link.queue_bytes(link) == 0
      assert {[], _link, nil} = Link.drain(link, 0)
    end
  end

  describe "loss in bursts" do
    test "takes loss_pct of the packets, loss_burst in a row on average" do
      for {pct, burst} <- [{5, 5}, {2, 4}, {20, 10}, {1, 2}] do
        link = Link.new(%Impairment{loss_pct: pct, loss_burst: burst}, 3, 0)
        {outcomes, _link} = burst(link, 400_000)
        runs = runs(outcomes)

        assert_in_delta 100 * Enum.sum(runs) / length(outcomes), pct, pct * 0.05
        assert_in_delta mean(runs), burst, burst * 0.05
      end
    end

    test "has runs of every length, the longer the rarer" do
      link = Link.new(%Impairment{loss_pct: 10, loss_burst: 4}, 3, 0)
      {outcomes, _link} = burst(link, 400_000)
      lengths = outcomes |> runs() |> Enum.frequencies()

      # A run goes on with the probability 3/4.
      for length <- 1..8 do
        assert_in_delta lengths[length + 1] / lengths[length], 0.75, 0.05
      end
    end

    test "takes one packet at a time at a loss_burst of 1" do
      link = Link.new(%Impairment{loss_pct: 5, loss_burst: 1}, 3, 0)
      {outcomes, _link} = burst(link, 400_000)
      runs = runs(outcomes)

      assert_in_delta 100 * Enum.sum(runs) / length(outcomes), 5, 0.25
      # Two in a row happen by chance: 1 / (1 - 0.05).
      assert_in_delta mean(runs), 1.053, 0.01
    end

    test "takes loss_pct in longer runs where loss_burst cannot" do
      link = Link.new(%Impairment{loss_pct: 80, loss_burst: 2}, 3, 0)
      {outcomes, _link} = burst(link, 400_000)
      runs = runs(outcomes)

      assert_in_delta 100 * Enum.sum(runs) / length(outcomes), 80, 1
      assert_in_delta mean(runs), 4, 0.1
    end

    test "repeats for a seed and differs between seeds" do
      outcomes = fn seed ->
        link = Link.new(%Impairment{loss_pct: 30, loss_burst: 3}, seed, 0)
        link |> burst(200) |> elem(0)
      end

      assert outcomes.(7) == outcomes.(7)
      assert outcomes.(7) != outcomes.(8)
    end

    test "goes on through a change of the impairment" do
      # Bad for good: r is 0 but for the rounding.
      stuck = %Impairment{loss_pct: 50, loss_burst: 1.0e12}
      link = Link.new(%Impairment{loss_pct: 99, loss_burst: 100}, 3, 0)
      {outcomes, link} = burst(link, 50)
      assert :lost in outcomes

      {outcomes, link} = link |> Link.apply(stuck, 0) |> burst(1_000)
      state = List.last(outcomes)
      assert Enum.uniq(Enum.take(outcomes, -900)) == [state]

      {outcomes, _link} = link |> Link.apply(%{stuck | queue_ms: 10}, 0) |> burst(100)
      assert Enum.uniq(outcomes) == [state]
    end
  end

  describe "a blackout" do
    test "refuses packets but lets the queue drain" do
      shaped = %Impairment{rate_kbps: 120, queue_ms: 10_000}
      {_outcomes, link} = burst(Link.new(shaped, 0, 0), 2)
      link = Link.apply(link, %{shaped | blackout?: true}, 0)

      assert {:blackout, link} = Link.offer(link, :late, @packet)
      assert {[1, 2], _link, nil} = Link.drain(link, 1_000)
    end

    test "takes packets again once lifted" do
      link = Link.new(%Impairment{blackout?: true}, 0, 0)
      assert {:blackout, link} = Link.offer(link, 1, @packet)

      link = Link.apply(link, %Impairment{}, 10)
      assert {:queued, link} = Link.offer(link, 2, @packet)
      assert {[2], _link, nil} = Link.drain(link, 10)
    end
  end

  describe "the delay" do
    test "holds packets for delay_ms and says when they are due" do
      link = Link.new(%Impairment{delay_ms: 30}, 0, 0)
      {_outcomes, link} = burst(link, 2)

      assert {[], link, 30} = Link.drain(link, 0)
      assert {[], link, 1} = Link.drain(link, 29)
      assert {[1, 2], _link, nil} = Link.drain(link, 30)
    end

    test "counts from when the bucket lets a packet go" do
      # The bucket has filled by 1000; the fourth packet waits for it 100 ms.
      link = Link.new(%Impairment{rate_kbps: 120, queue_ms: 10_000, delay_ms: 30}, 0, 0)
      {_outcomes, link} = burst(link, 4)

      assert {[], link, 30} = Link.drain(link, 1_000)
      assert {[1, 2, 3], link, 70} = Link.drain(link, 1_030)
      assert {[], link, 30} = Link.drain(link, 1_100)
      assert {[4], _link, nil} = Link.drain(link, 1_130)
    end

    test "comes back for whichever is sooner, the bucket or the delay" do
      link = Link.new(%Impairment{rate_kbps: 120, queue_ms: 10_000, delay_ms: 500}, 0, 0)
      {_outcomes, link} = burst(link, 4)

      assert {[], _link, 100} = Link.drain(link, 1_000)
    end

    test "a shorter one does not overtake packets held for a longer one" do
      link = Link.new(%Impairment{delay_ms: 100}, 0, 0)
      {:queued, link} = Link.offer(link, 1, @packet)
      assert {[], link, 100} = Link.drain(link, 0)

      link = Link.apply(link, %Impairment{delay_ms: 10}, 5)
      {:queued, link} = Link.offer(link, 2, @packet)

      assert {[], link, 95} = Link.drain(link, 5)
      assert {[], link, 50} = Link.drain(link, 50)
      assert {[1, 2], _link, nil} = Link.drain(link, 100)
    end

    test "is not counted as queued" do
      link = Link.new(%Impairment{delay_ms: 30}, 0, 0)
      {_outcomes, link} = burst(link, 2)
      {[], link, 30} = Link.drain(link, 0)

      assert Link.queue_bytes(link) == 0
    end
  end

  describe "the jitter" do
    # Further apart than any delay drawn: no packet waits for another.
    @apart 200

    test "spreads the delay around delay_ms with the deviation jitter_ms" do
      for {delay, jitter} <- [{50, 10}, {40, 5}, {100, 1}] do
        link = Link.new(%Impairment{delay_ms: delay, jitter_ms: jitter}, 5, 0)
        held = link |> held(Enum.map(0..19_999, &(&1 * @apart))) |> Enum.map(&elem(&1, 1))

        # The link is drained on whole milliseconds, after the packet is due.
        assert_in_delta mean(held), delay + 0.5, 0.25
        assert_in_delta deviation(held), jitter, jitter * 0.05 + 0.1
      end
    end

    test "holds no packet for less than no time" do
      link = Link.new(%Impairment{delay_ms: 5, jitter_ms: 20}, 5, 0)
      held = link |> held(Enum.map(0..19_999, &(&1 * @apart))) |> Enum.map(&elem(&1, 1))

      assert Enum.min(held) == 0
      assert Enum.count(held, &(&1 == 0)) > 1_000
    end

    test "keeps the packets in order, holding the close ones longer" do
      # A packet every 5 ms, as 2000 kbit/s of 1200-byte ones.
      link = Link.new(%Impairment{delay_ms: 40, jitter_ms: 10}, 5, 0)
      left = held(link, Enum.map(0..19_999, &(&1 * 5)))

      assert Enum.map(left, &elem(&1, 0)) == Enum.to_list(0..19_999)
      assert mean(Enum.map(left, &elem(&1, 1))) > 45
    end

    test "repeats for a seed and differs between seeds" do
      held = fn seed ->
        link = Link.new(%Impairment{delay_ms: 50, jitter_ms: 10}, seed, 0)
        held(link, Enum.map(0..99, &(&1 * @apart)))
      end

      assert held.(7) == held.(7)
      assert held.(7) != held.(8)
    end

    test "does not change what is lost" do
      lossy = %Impairment{loss_pct: 30, loss_burst: 3, delay_ms: 50}

      outcomes = fn impairment ->
        {outcomes, _link} =
          Enum.map_reduce(1..500, Link.new(impairment, 7, 0), fn packet, link ->
            {outcome, link} = Link.offer(link, packet, @packet)
            {_left, link, _wait_ms} = Link.drain(link, packet * 10)
            {outcome, link}
          end)

        outcomes
      end

      assert outcomes.(lossy) == outcomes.(%{lossy | jitter_ms: 20})
    end
  end
end
