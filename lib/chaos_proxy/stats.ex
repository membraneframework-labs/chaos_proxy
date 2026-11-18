defmodule ChaosProxy.Stats do
  @moduledoc false
  # The counters of a `ChaosProxy` as plain data: totals and one bucket per
  # second, per direction. The second is passed in (`rotate/2`), counted from
  # the proxy's start.

  @type t :: %__MODULE__{
          history: pos_integer() | :infinity,
          totals: %{up: ChaosProxy.counters(), down: ChaosProxy.counters()},
          current: ChaosProxy.second(),
          seconds: [ChaosProxy.second()]
        }

  @typedoc "The counters that add up; `queue_bytes_max` is a peak, see `peak/3`."
  @type sum ::
          :offered_bytes
          | :forwarded_bytes
          | :forwarded_packets
          | :dropped_packets
          | :lost_packets
          | :blackout_packets
          | :refused_packets

  @enforce_keys [:history, :totals, :current]
  defstruct @enforce_keys ++ [seconds: []]

  @spec new(pos_integer() | :infinity) :: t()
  def new(history) do
    %__MODULE__{
      history: history,
      totals: %{up: empty_counters(), down: empty_counters()},
      current: empty_second(0)
    }
  end

  @doc """
  Moves on to the bucket of `second`, archiving the previous one, and says
  whether that was another second.
  """
  @spec rotate(t(), non_neg_integer()) :: {:same | :rolled, t()}
  def rotate(%__MODULE__{current: %{second: second}} = stats, second), do: {:same, stats}

  def rotate(%__MODULE__{} = stats, second) do
    {:rolled, %{stats | seconds: keep(stats), current: empty_second(second)}}
  end

  @spec add(t(), ChaosProxy.direction(), sum(), non_neg_integer()) :: t()
  def add(%__MODULE__{} = stats, direction, key, amount),
    do: bump(stats, direction, &Map.update!(&1, key, fn sum -> sum + amount end))

  @doc "Records how many bytes are queued, for `queue_bytes_max`."
  @spec peak(t(), ChaosProxy.direction(), non_neg_integer()) :: t()
  def peak(%__MODULE__{} = stats, direction, queue_bytes) do
    bump(stats, direction, fn counters ->
      %{counters | queue_bytes_max: max(counters.queue_bytes_max, queue_bytes)}
    end)
  end

  @doc "The buckets oldest first, the current one included, and the totals."
  @spec report(t()) :: %{
          seconds: [ChaosProxy.second()],
          totals: %{up: ChaosProxy.counters(), down: ChaosProxy.counters()}
        }
  def report(%__MODULE__{} = stats),
    do: %{seconds: Enum.reverse([stats.current | stats.seconds]), totals: stats.totals}

  @spec bump(t(), ChaosProxy.direction(), (ChaosProxy.counters() -> ChaosProxy.counters())) ::
          t()
  defp bump(stats, direction, fun) do
    stats = update_in(stats.current[direction], fun)
    update_in(stats.totals[direction], fun)
  end

  @spec keep(t()) :: [ChaosProxy.second()]
  defp keep(%__MODULE__{history: :infinity} = stats), do: [stats.current | stats.seconds]
  defp keep(stats), do: Enum.take([stats.current | stats.seconds], stats.history - 1)

  @spec empty_second(non_neg_integer()) :: ChaosProxy.second()
  defp empty_second(second), do: %{second: second, up: empty_counters(), down: empty_counters()}

  @spec empty_counters() :: ChaosProxy.counters()
  defp empty_counters,
    do: %{
      offered_bytes: 0,
      forwarded_bytes: 0,
      forwarded_packets: 0,
      dropped_packets: 0,
      lost_packets: 0,
      blackout_packets: 0,
      queue_bytes_max: 0,
      refused_packets: 0
    }
end
