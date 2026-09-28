defmodule ChaosProxy.Impairment do
  @moduledoc """
  How one direction of a `ChaosProxy` treats packets. The knobs apply in this
  order:

    * `blackout?` - drops everything, as a link outage does
    * `loss_pct` - random loss, an integer percentage
    * `rate_kbps` - rate limit, above 0; `:infinity` for none. A burst of
      20 ms worth of the rate passes at once, and never less than three
      1500-byte packets.
    * `queue_ms` - how much may wait for the rate limit, as time at
      `rate_kbps`, and never less than eight 1500-byte packets. What does not
      fit is dropped.
    * `delay_ms` - one-way delay. Packets keep their order: once the delay is
      lowered, none overtakes those still held for the longer one.

  The default is a transparent link.
  """

  @type t :: %__MODULE__{
          blackout?: boolean(),
          loss_pct: 0..100,
          rate_kbps: pos_integer() | float() | :infinity,
          queue_ms: pos_integer(),
          delay_ms: non_neg_integer()
        }

  defstruct blackout?: false,
            loss_pct: 0,
            rate_kbps: :infinity,
            queue_ms: 200,
            delay_ms: 0

  # `ChaosProxy.apply/2`'s argument as a map of the directions it sets.
  @doc false
  @spec split(ChaosProxy.impairments()) :: %{optional(ChaosProxy.direction()) => t()}
  def split(%__MODULE__{} = down) do
    rate!(down)
    %{down: down, up: %__MODULE__{delay_ms: down.delay_ms, blackout?: down.blackout?}}
  end

  def split(impairments) when is_list(impairments) do
    Enum.each(impairments, fn {_direction, impairment} -> rate!(impairment) end)
    Map.new(impairments)
  end

  @spec rate!(t()) :: :ok
  defp rate!(%__MODULE__{rate_kbps: :infinity}), do: :ok
  defp rate!(%__MODULE__{rate_kbps: rate}) when is_number(rate) and rate > 0, do: :ok

  defp rate!(%__MODULE__{rate_kbps: rate}),
    do: raise(ArgumentError, ":rate_kbps must be above 0 or :infinity, got: #{inspect(rate)}")
end
