defmodule ChaosProxy.Link do
  @moduledoc """
  One direction of a `ChaosProxy` as plain data: blackout and random loss on
  the way in, then a token bucket draining a FIFO with tail drop. It holds
  packets, never sends them.

  Time is passed in as milliseconds on any monotonic clock, so a link can be
  driven and tested without timers. `delay_ms` is not applied here: the
  caller holds each packet that `drain/2` releases for that long.

      link = Link.new(%Impairment{rate_kbps: 800}, 7, now)
      {:queued, link} = Link.offer(link, packet, byte_size(data), now)
      {released, link, wait_ms} = Link.drain(link, now)

  The bucket holds 20 ms worth of the rate, but never less than three
  1500-byte packets, and the queue `queue_ms` worth, but never less than eight
  such packets, so that a very low rate still passes full-size datagrams.
  """

  alias ChaosProxy.Impairment

  @mtu 1500

  @type packet :: term()
  @type outcome :: :blackout | :lost | :dropped | :queued

  @type t :: %__MODULE__{
          impairment: Impairment.t(),
          rng: :rand.state(),
          queue: :queue.queue({packet(), non_neg_integer()}),
          queue_bytes: non_neg_integer(),
          tokens: float(),
          refilled_at: number()
        }

  @enforce_keys [:impairment, :rng, :queue, :refilled_at]
  defstruct [:impairment, :rng, :queue, :refilled_at, queue_bytes: 0, tokens: 0.0]

  @doc "A link with an empty queue and bucket; `seed` makes its losses repeatable."
  @spec new(Impairment.t(), integer(), number()) :: t()
  def new(%Impairment{} = impairment, seed, now) do
    %__MODULE__{
      impairment: impairment,
      rng: :rand.seed_s(:exsss, {seed, seed + 1, seed + 2}),
      queue: :queue.new(),
      refilled_at: now
    }
  end

  @doc """
  Changes the knobs. Queued packets stay and drain at the new rate; call
  `drain/2` afterwards.
  """
  @spec apply(t(), Impairment.t(), number()) :: t()
  def apply(link, %Impairment{} = impairment, now) do
    link = %{refill(link, now) | impairment: impairment}
    %{link | tokens: min(link.tokens, burst_bytes(impairment))}
  end

  @doc "Takes a packet of `size` bytes in, or says why not."
  @spec offer(t(), packet(), non_neg_integer(), number()) :: {outcome(), t()}
  def offer(%__MODULE__{impairment: %Impairment{blackout?: true}} = link, _packet, _size, _now),
    do: {:blackout, link}

  def offer(link, packet, size, _now) do
    {lost?, link} = lost?(link)

    cond do
      lost? ->
        {:lost, link}

      link.queue_bytes + size > queue_cap_bytes(link.impairment) ->
        {:dropped, link}

      true ->
        queue = :queue.in({packet, size}, link.queue)
        {:queued, %{link | queue: queue, queue_bytes: link.queue_bytes + size}}
    end
  end

  @doc """
  Releases the packets the bucket can pay for, oldest first, and says in how
  many milliseconds the next one can go (`nil` when the queue is empty).
  """
  @spec drain(t(), number()) :: {[packet()], t(), pos_integer() | nil}
  def drain(link, now), do: release(refill(link, now), [])

  @doc "Bytes waiting in the queue."
  @spec queue_bytes(t()) :: non_neg_integer()
  def queue_bytes(link), do: link.queue_bytes

  defp release(link, released) do
    case :queue.peek(link.queue) do
      :empty ->
        {Enum.reverse(released), link, nil}

      {:value, {packet, size}} ->
        if affordable?(link, size) do
          link = %{
            link
            | queue: :queue.drop(link.queue),
              queue_bytes: link.queue_bytes - size,
              tokens: spend(link, size)
          }

          release(link, [packet | released])
        else
          wait_ms = ceil((size - link.tokens) / bytes_per_ms(link.impairment))
          {Enum.reverse(released), link, max(wait_ms, 1)}
        end
    end
  end

  defp affordable?(%{impairment: %Impairment{rate_kbps: :infinity}}, _size), do: true
  defp affordable?(link, size), do: link.tokens >= size

  defp spend(%{impairment: %Impairment{rate_kbps: :infinity}} = link, _size), do: link.tokens
  defp spend(link, size), do: link.tokens - size

  defp lost?(%{impairment: %Impairment{loss_pct: pct}} = link) when pct <= 0, do: {false, link}

  defp lost?(%{impairment: %Impairment{loss_pct: pct}} = link) do
    {x, rng} = :rand.uniform_s(link.rng)
    {x * 100 < pct, %{link | rng: rng}}
  end

  defp refill(%{impairment: %Impairment{rate_kbps: :infinity}} = link, now),
    do: %{link | refilled_at: now}

  defp refill(link, now) do
    %{impairment: impairment} = link
    tokens = link.tokens + (now - link.refilled_at) * bytes_per_ms(impairment)
    %{link | tokens: min(tokens, burst_bytes(impairment)), refilled_at: now}
  end

  defp bytes_per_ms(%Impairment{rate_kbps: rate}), do: rate / 8

  defp burst_bytes(%Impairment{rate_kbps: :infinity}), do: 0.0
  defp burst_bytes(impairment), do: max(3 * @mtu, bytes_per_ms(impairment) * 20)

  defp queue_cap_bytes(%Impairment{rate_kbps: :infinity}), do: :infinity

  defp queue_cap_bytes(%Impairment{queue_ms: queue_ms} = impairment),
    do: max(8 * @mtu, round(bytes_per_ms(impairment) * queue_ms))
end
