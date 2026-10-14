defmodule ChaosProxy.StatsTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ChaosProxy.Stats

  # One packet of `bytes` offered and forwarded on the downlink in `second`.
  defp forward(stats, second, bytes) do
    {_rolled, stats} = Stats.rotate(stats, second)

    stats
    |> Stats.add(:down, :offered_bytes, bytes)
    |> Stats.add(:down, :forwarded_bytes, bytes)
  end

  test "starts with an empty bucket for second 0" do
    assert %{seconds: [%{second: 0, up: up, down: down}], totals: %{up: up, down: down}} =
             Stats.report(Stats.new(300))

    assert up == down
    assert Enum.uniq(Map.values(up)) == [0]
  end

  test "counts per direction, in the current second and in total" do
    stats =
      Stats.new(300)
      |> Stats.add(:up, :offered_bytes, 5)
      |> Stats.add(:up, :offered_bytes, 7)
      |> Stats.add(:down, :lost_packets, 1)

    assert %{seconds: [second], totals: totals} = Stats.report(stats)
    assert %{up: %{offered_bytes: 12, lost_packets: 0}} = second
    assert %{down: %{offered_bytes: 0, lost_packets: 1}} = second
    assert Map.take(second, [:up, :down]) == totals
  end

  test "keeps the peak of the queue, per second and overall" do
    stats = Stats.new(300) |> Stats.peak(:down, 3_000) |> Stats.peak(:down, 1_500)
    {:rolled, stats} = Stats.rotate(stats, 1)
    stats = Stats.peak(stats, :down, 1_000)

    assert %{seconds: [first, second], totals: totals} = Stats.report(stats)
    assert first.down.queue_bytes_max == 3_000
    assert second.down.queue_bytes_max == 1_000
    assert totals.down.queue_bytes_max == 3_000
    assert totals.up.queue_bytes_max == 0
  end

  test "says whether a second rolled over" do
    stats = Stats.new(300)

    assert {:same, ^stats} = Stats.rotate(stats, 0)
    assert {:rolled, stats} = Stats.rotate(stats, 1)
    assert {:same, ^stats} = Stats.rotate(stats, 1)
  end

  property "reports the last `history` seconds oldest first, and all of them in the totals" do
    check all history <- one_of([integer(1..20), constant(:infinity)]),
              gaps <- list_of(integer(1..5), max_length: 50) do
      seconds = Enum.scan([0 | gaps], &+/2)
      # A second's bytes tell it apart from the others.
      stats = Enum.reduce(seconds, Stats.new(history), &forward(&2, &1, &1 + 1))
      kept = if history == :infinity, do: seconds, else: Enum.take(seconds, -history)

      assert %{seconds: reported, totals: %{down: %{forwarded_bytes: total}}} =
               Stats.report(stats)

      assert Enum.map(reported, &{&1.second, &1.down.forwarded_bytes}) ==
               Enum.map(kept, &{&1, &1 + 1})

      assert total == seconds |> Enum.map(&(&1 + 1)) |> Enum.sum()
    end
  end
end
