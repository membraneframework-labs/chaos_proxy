defmodule ChaosProxy.Impairment do
  @moduledoc """
  How one direction of a `ChaosProxy` treats packets. The knobs apply in this
  order:

    * `blackout?` - drops everything, as a link outage does
    * `loss_pct` - random loss, an integer percentage
    * `loss_burst` - how many packets in a row are lost, on average; 1 or
      more. At 1 every packet is lost on its own, with the probability
      `loss_pct`. Bursts this short cannot lose more than
      100 * `loss_burst` / (`loss_burst` + 1) percent; above that, they are
      as long as `loss_pct` takes.
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

  # The Gilbert model behind a `loss_burst` above 1, as `{p, r}`: a link is
  # good or bad, and loses what it is offered while bad and nothing while
  # good. After every packet a good link turns bad with the probability `p`
  # and a bad one good with `r`. A bad spell lasts 1 / r packets on average,
  # and p / (p + r) of the packets meet one. `p` cannot go above 1, so past
  # that point `r` drops instead and the spells grow.
  @doc false
  @spec gilbert(t()) :: {p :: float(), r :: float()}
  def gilbert(%__MODULE__{loss_pct: pct, loss_burst: burst}) when pct > 0 and pct < 100 do
    odds = pct / (100 - pct)
    r = 1 / max(burst, odds)
    {r * odds, r}
  end

  @expected [
    blackout?: "a boolean",
    loss_pct: "an integer from 0 to 100",
    loss_burst: "1 or more",
    rate_kbps: "above 0 or :infinity",
    queue_ms: "a positive integer",
    delay_ms: "a non-negative integer",
    jitter_ms: "a non-negative integer"
  ]

  # `ChaosProxy.apply/2`'s argument, checked, as a map of the directions it
  # sets.
  @doc false
  @spec parse(term()) ::
          {:ok, %{optional(ChaosProxy.direction()) => t()}} | {:error, String.t()}
  def parse(%__MODULE__{} = down) do
    with :ok <- check(down) do
      up = %__MODULE__{
        delay_ms: down.delay_ms,
        jitter_ms: down.jitter_ms,
        blackout?: down.blackout?
      }

      {:ok, %{down: down, up: up}}
    end
  end

  def parse(impairments) when is_list(impairments) do
    Enum.reduce_while(impairments, {:ok, %{}}, fn
      {direction, %__MODULE__{} = impairment}, {:ok, parsed} when direction in [:up, :down] ->
        case check(impairment) do
          :ok -> {:cont, {:ok, Map.put(parsed, direction, impairment)}}
          error -> {:halt, error}
        end

      other, _parsed ->
        {:halt,
         {:error, "expected {:up | :down, %ChaosProxy.Impairment{}}, got: #{inspect(other)}"}}
    end)
  end

  def parse(other) do
    {:error,
     "impairments must be a %ChaosProxy.Impairment{} or a keyword list of them, got: " <>
       inspect(other)}
  end

  @spec check(t()) :: :ok | {:error, String.t()}
  defp check(impairment) do
    case Enum.find(@expected, fn {key, _expected} ->
           not valid?(key, Map.fetch!(impairment, key))
         end) do
      nil ->
        :ok

      {key, expected} ->
        {:error,
         "#{inspect(key)} must be #{expected}, got: #{inspect(Map.fetch!(impairment, key))}"}
    end
  end

  @spec valid?(atom(), term()) :: boolean()
  defp valid?(:blackout?, blackout?), do: is_boolean(blackout?)
  defp valid?(:loss_pct, pct), do: pct in 0..100
  defp valid?(:loss_burst, burst), do: is_number(burst) and burst >= 1
  defp valid?(:rate_kbps, rate), do: rate == :infinity or (is_number(rate) and rate > 0)
  defp valid?(:queue_ms, queue_ms), do: is_integer(queue_ms) and queue_ms > 0
  defp valid?(:delay_ms, delay_ms), do: is_integer(delay_ms) and delay_ms >= 0
  defp valid?(:jitter_ms, jitter_ms), do: is_integer(jitter_ms) and jitter_ms >= 0
end
