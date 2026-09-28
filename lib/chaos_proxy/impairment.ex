defmodule ChaosProxy.Impairment do
  @moduledoc """
  The knobs of one direction of a `ChaosProxy`, applied in this order:

    * `blackout?` - drop everything (a link outage)
    * `loss_pct` - random loss, 0 to 100
    * `rate_kbps` - token-bucket rate, `:infinity` for no limit (and no queue)
    * `queue_ms` - how much queueing at `rate_kbps` fits before tail drop
    * `delay_ms` - one-way delay added on the way out

  The default is a transparent link.
  """

  @type t :: %__MODULE__{
          blackout?: boolean(),
          loss_pct: number(),
          rate_kbps: pos_integer() | float() | :infinity,
          queue_ms: pos_integer(),
          delay_ms: non_neg_integer()
        }

  defstruct blackout?: false,
            loss_pct: 0.0,
            rate_kbps: :infinity,
            queue_ms: 200,
            delay_ms: 0
end
