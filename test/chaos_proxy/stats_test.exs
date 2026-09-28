defmodule ChaosProxy.StatsTest do
  use ExUnit.Case, async: true

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

    assert Enum.sort(Map.keys(up)) == [
             :blackout_packets,
             :dropped_packets,
             :forwarded_bytes,
             :lost_packets,
             :offered_bytes,
             :queue_bytes_max,
             :refused_packets
           ]
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

  test "reports the seconds oldest first, without the ones nothing happened in" do
    stats = Stats.new(300) |> forward(0, 1) |> forward(1, 2) |> forward(5, 3)

    assert %{seconds: seconds, totals: %{down: %{forwarded_bytes: 6}}} = Stats.report(stats)
    assert Enum.map(seconds, &{&1.second, &1.down.forwarded_bytes}) == [{0, 1}, {1, 2}, {5, 3}]
  end

  test "history bounds the seconds, the current one included, but not the totals" do
    stats = Enum.reduce(0..9, Stats.new(3), &forward(&2, &1, 10))

    assert %{seconds: seconds, totals: %{down: %{forwarded_bytes: 100}}} = Stats.report(stats)
    assert Enum.map(seconds, & &1.second) == [7, 8, 9]
  end

  test "a history of one keeps the current second only" do
    stats = Enum.reduce(0..3, Stats.new(1), &forward(&2, &1, 10))

    assert %{seconds: [%{second: 3}]} = Stats.report(stats)
  end

  test "an infinite history keeps every second" do
    stats = Enum.reduce(0..999, Stats.new(:infinity), &forward(&2, &1, 10))

    assert %{seconds: seconds} = Stats.report(stats)
    assert Enum.map(seconds, & &1.second) == Enum.to_list(0..999)
  end
end
