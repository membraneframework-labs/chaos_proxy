defmodule ChaosProxy.Impairment do
  @moduledoc """
  How one direction of a `ChaosProxy` treats packets. The knobs apply in this
  order:

    * `blackout?` - drops everything, as a link outage does
    * `loss_pct` - random loss, an integer percentage
    * `loss_burst` - how many packets in a row are lost, on average; 1 or
      more. At 1 every packet is lost on its own, with the probability
      `loss_pct`. Above 1 the losses come from the Gilbert model, see
      `gilbert/1`.
    * `rate_kbps` - rate limit, above 0; `:infinity` for none. A burst of
      20 ms worth of the rate passes at once, and never less than three
      1500-byte packets.
    * `queue_ms` - how much may wait for the rate limit, as time at
      `rate_kbps`, and never less than eight 1500-byte packets. What does not
      fit is dropped.
    * `delay_ms` - one-way delay. Packets keep their order: once the delay is
      lowered, none overtakes those still held for the longer one.
    * `jitter_ms` - the standard deviation of the delay, which is drawn for
      every packet from a normal distribution around `delay_ms`, and is never
      below 0. Packets keep their order here too: one that drew a shorter
      delay leaves with the one before it. Packets closer to each other than
      the jitter are therefore held longer than `delay_ms` on average.

  The default is a transparent link.
  """

  @type t :: %__MODULE__{
          blackout?: boolean(),
          loss_pct: 0..100,
          loss_burst: pos_integer() | float(),
          rate_kbps: pos_integer() | float() | :infinity,
          queue_ms: pos_integer(),
          delay_ms: non_neg_integer(),
          jitter_ms: non_neg_integer()
        }

  defstruct blackout?: false,
            loss_pct: 0,
            loss_burst: 1,
            rate_kbps: :infinity,
            queue_ms: 200,
            delay_ms: 0,
            jitter_ms: 0

  @doc """
  The Gilbert model behind a `loss_burst` above 1, as `{p, r}`: a link is
  good or bad, and loses what it is offered while bad and nothing while good.
  After every packet a good link turns bad with the probability `p` and a bad
  one good with `r`.

  A bad spell lasts 1 / r packets on average, which is `loss_burst`, and
  p / (p + r) of the packets meet one, which is `loss_pct`. Bursts that short
  cannot lose that much once `loss_pct` is above
  100 * `loss_burst` / (`loss_burst` + 1): the bursts are then as long as
  `loss_pct` takes.

      iex> ChaosProxy.Impairment.gilbert(%ChaosProxy.Impairment{loss_pct: 20, loss_burst: 4})
      {0.0625, 0.25}

      iex> ChaosProxy.Impairment.gilbert(%ChaosProxy.Impairment{loss_pct: 80, loss_burst: 2})
      {1.0, 0.25}
  """
  @spec gilbert(t()) :: {p :: float(), r :: float()}
  def gilbert(%__MODULE__{loss_pct: pct, loss_burst: burst}) when pct > 0 and pct < 100 do
    odds = pct / (100 - pct)
    r = 1 / max(burst, odds)
    {r * odds, r}
  end

  # `ChaosProxy.apply/2`'s argument as a map of the directions it sets.
  @doc false
  @spec split(ChaosProxy.impairments()) :: %{optional(ChaosProxy.direction()) => t()}
  def split(%__MODULE__{} = down) do
    check!(down)

    %{
      down: down,
      up: %__MODULE__{
        delay_ms: down.delay_ms,
        jitter_ms: down.jitter_ms,
        blackout?: down.blackout?
      }
    }
  end

  def split(impairments) when is_list(impairments) do
    Enum.each(impairments, fn {_direction, impairment} -> check!(impairment) end)
    Map.new(impairments)
  end

  @spec check!(t()) :: :ok
  defp check!(%__MODULE__{loss_burst: burst} = impairment)
       when is_number(burst) and burst >= 1,
       do: rate!(impairment)

  defp check!(%__MODULE__{loss_burst: burst}),
    do: raise(ArgumentError, ":loss_burst must be 1 or more, got: #{inspect(burst)}")

  @spec rate!(t()) :: :ok
  defp rate!(%__MODULE__{rate_kbps: :infinity}), do: :ok
  defp rate!(%__MODULE__{rate_kbps: rate}) when is_number(rate) and rate > 0, do: :ok

  defp rate!(%__MODULE__{rate_kbps: rate}),
    do: raise(ArgumentError, ":rate_kbps must be above 0 or :infinity, got: #{inspect(rate)}")
end
