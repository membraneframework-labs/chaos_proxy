defmodule ChaosProxy.Impairment do
  @moduledoc """
  One setting of the chaos proxy's knobs, in force from `at_ms` until the next
  one in a scenario's schedule.

    * `rate_kbps` - token-bucket rate of the relay -> subscriber direction
    * `delay_ms` - one-way delay in both directions
    * `loss_pct` - random loss of relay -> subscriber packets
    * `queue_ms` - queueing before tail drop, at `rate_kbps`
    * `blackout?` - drop everything in both directions (a link outage)
  """

  @type t :: %__MODULE__{
          at_ms: non_neg_integer(),
          rate_kbps: number(),
          delay_ms: non_neg_integer(),
          loss_pct: float(),
          queue_ms: pos_integer(),
          blackout?: boolean()
        }

  if Code.ensure_loaded?(Jason.Encoder) do
    @derive Jason.Encoder
  end

  defstruct at_ms: 0,
            rate_kbps: 4_000,
            delay_ms: 50,
            loss_pct: 0.0,
            queue_ms: 200,
            blackout?: false

  @doc "A link good enough that the tracks in a scenario never queue."
  @spec clean(non_neg_integer()) :: t()
  def clean(at_ms),
    do: %__MODULE__{at_ms: at_ms, rate_kbps: 8_000, delay_ms: 20, loss_pct: 0.0, queue_ms: 200}

  @doc "Whether the link degrades traffic at all (queueing or loss possible)."
  @spec clean?(t(), pos_integer()) :: boolean()
  def clean?(%__MODULE__{} = i, demand_kbps),
    do: not i.blackout? and i.loss_pct == 0.0 and i.rate_kbps >= 2 * demand_kbps

  @spec from_map(map()) :: t()
  def from_map(map) do
    %__MODULE__{
      at_ms: map["at_ms"],
      rate_kbps: map["rate_kbps"],
      delay_ms: map["delay_ms"],
      loss_pct: map["loss_pct"] / 1,
      queue_ms: map["queue_ms"],
      blackout?: map["blackout?"]
    }
  end
end
