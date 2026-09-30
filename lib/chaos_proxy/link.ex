defmodule ChaosProxy.Link do
  @moduledoc """
  One direction of a `ChaosProxy` on its own, as plain data. It applies a
  `ChaosProxy.Impairment` to the packets it is offered and holds them until
  they may leave; it never sends them.

  Time is an argument, in milliseconds on any monotonic clock, so a link can
  be driven by a simulation or a test.

      link = Link.new(%Impairment{rate_kbps: 800, delay_ms: 20}, 7, now)
      {:queued, link} = Link.offer(link, packet, byte_size(data))
      {leaving, link, wait_ms} = Link.drain(link, now)
  """

  alias ChaosProxy.Impairment

  @mtu 1500

  @type packet :: term()
  @typedoc """
  What became of an offered packet: it is held (`:queued`), or gone to a
  blackout, to loss or for want of room in the queue (`:dropped`).
  """
  @type outcome :: :blackout | :lost | :dropped | :queued

  @type t :: %__MODULE__{
          impairment: Impairment.t(),
          rng: :rand.state(),
          jitter_rng: :rand.state(),
          bad?: boolean(),
          queue: :queue.queue({packet(), non_neg_integer()}),
          queue_bytes: non_neg_integer(),
          tokens: number(),
          refilled_at: number(),
          delayed: :queue.queue({due :: number(), packet()})
        }

  @enforce_keys [:impairment, :rng, :jitter_rng, :queue, :delayed, :refilled_at]
  defstruct @enforce_keys ++ [queue_bytes: 0, tokens: 0.0, bad?: false]

  @doc "A link holding nothing. `seed` makes its losses and its jitter repeatable."
  @spec new(Impairment.t(), integer(), number()) :: t()
  def new(%Impairment{} = impairment, seed, now) do
    %__MODULE__{
      impairment: impairment,
      rng: :rand.seed_s(:exsss, {seed, seed + 1, seed + 2}),
      # Of its own, so that the jitter does not change what is lost.
      jitter_rng: :rand.seed_s(:exsss, {seed + 3, seed + 4, seed + 5}),
      queue: :queue.new(),
      delayed: :queue.new(),
      refilled_at: now
    }
  end

  @doc """
  Changes the impairment. Call `drain/2` afterwards: packets it holds may
  leave sooner.
  """
  @spec apply(t(), Impairment.t(), number()) :: t()
  def apply(link, %Impairment{} = impairment, now) do
    link = %{refill(link, now) | impairment: impairment}
    %{link | tokens: min(link.tokens, burst_bytes(impairment))}
  end

  @doc "Takes a packet of `size` bytes in, or says why not."
  @spec offer(t(), packet(), non_neg_integer()) :: {outcome(), t()}
  def offer(%__MODULE__{impairment: %Impairment{blackout?: true}} = link, _packet, _size),
    do: {:blackout, link}

  def offer(link, packet, size) do
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
  Returns the packets that may leave at `now`, oldest first, and in how many
  milliseconds to call again (`nil` when the link holds nothing).
  """
  @spec drain(t(), number()) :: {[packet()], t(), pos_integer() | nil}
  def drain(link, now) do
    {link, bucket_wait_ms} = link |> refill(now) |> release(now)
    {due, link, delay_wait_ms} = due(link, now, [])
    {due, link, sooner(bucket_wait_ms, delay_wait_ms)}
  end

  @doc "Bytes waiting for the rate limit."
  @spec queue_bytes(t()) :: non_neg_integer()
  def queue_bytes(link), do: link.queue_bytes

  @spec release(t(), number()) :: {t(), pos_integer() | nil}
  defp release(link, now) do
    case :queue.peek(link.queue) do
      :empty ->
        {link, nil}

      {:value, {packet, size}} ->
        if affordable?(link, size) do
          {delay_ms, link} = delay_ms(link)

          release(
            %{
              link
              | queue: :queue.drop(link.queue),
                queue_bytes: link.queue_bytes - size,
                tokens: spend(link, size),
                delayed: :queue.in({now + delay_ms, packet}, link.delayed)
            },
            now
          )
        else
          {link, max(ceil((size - link.tokens) / bytes_per_ms(link.impairment)), 1)}
        end
    end
  end

  @spec delay_ms(t()) :: {number(), t()}
  defp delay_ms(%{impairment: %Impairment{jitter_ms: 0, delay_ms: delay_ms}} = link),
    do: {delay_ms, link}

  defp delay_ms(%{impairment: %Impairment{jitter_ms: jitter_ms, delay_ms: delay_ms}} = link) do
    {delay_ms, rng} = :rand.normal_s(delay_ms, jitter_ms * jitter_ms, link.jitter_rng)
    {max(delay_ms, 0), %{link | jitter_rng: rng}}
  end

  # The packets leave in the order they were delayed in, so one due before
  # the packet ahead of it waits for that one.
  @spec due(t(), number(), [packet()]) :: {[packet()], t(), pos_integer() | nil}
  defp due(link, now, due) do
    case :queue.peek(link.delayed) do
      {:value, {at, packet}} when at <= now ->
        due(%{link | delayed: :queue.drop(link.delayed)}, now, [packet | due])

      {:value, {at, _packet}} ->
        {Enum.reverse(due), link, ceil(at - now)}

      :empty ->
        {Enum.reverse(due), link, nil}
    end
  end

  @spec sooner(pos_integer() | nil, pos_integer() | nil) :: pos_integer() | nil
  defp sooner(nil, wait_ms), do: wait_ms
  defp sooner(wait_ms, nil), do: wait_ms
  defp sooner(one, other), do: min(one, other)

  @spec affordable?(t(), non_neg_integer()) :: boolean()
  defp affordable?(%{impairment: %Impairment{rate_kbps: :infinity}}, _size), do: true
  defp affordable?(link, size), do: link.tokens >= size

  @spec spend(t(), non_neg_integer()) :: number()
  defp spend(%{impairment: %Impairment{rate_kbps: :infinity}} = link, _size), do: link.tokens
  defp spend(link, size), do: link.tokens - size

  @spec lost?(t()) :: {boolean(), t()}
  defp lost?(%{impairment: %Impairment{loss_pct: pct}} = link) when pct <= 0, do: {false, link}

  defp lost?(%{impairment: %Impairment{loss_pct: pct, loss_burst: burst}} = link)
       when burst == 1 or pct >= 100 do
    {x, rng} = :rand.uniform_s(link.rng)
    {x * 100 < pct, %{link | rng: rng}}
  end

  defp lost?(link) do
    {p, r} = Impairment.gilbert(link.impairment)
    {x, rng} = :rand.uniform_s(link.rng)
    turns? = if link.bad?, do: x < r, else: x < p
    {link.bad?, %{link | rng: rng, bad?: link.bad? != turns?}}
  end

  @spec refill(t(), number()) :: t()
  defp refill(%{impairment: %Impairment{rate_kbps: :infinity}} = link, now),
    do: %{link | refilled_at: now}

  defp refill(link, now) do
    %{impairment: impairment} = link
    tokens = link.tokens + (now - link.refilled_at) * bytes_per_ms(impairment)
    %{link | tokens: min(tokens, burst_bytes(impairment)), refilled_at: now}
  end

  @spec bytes_per_ms(Impairment.t()) :: float()
  defp bytes_per_ms(%Impairment{rate_kbps: rate}), do: rate / 8

  @spec burst_bytes(Impairment.t()) :: number()
  defp burst_bytes(%Impairment{rate_kbps: :infinity}), do: 0.0
  defp burst_bytes(impairment), do: max(3 * @mtu, bytes_per_ms(impairment) * 20)

  @spec queue_cap_bytes(Impairment.t()) :: pos_integer() | :infinity
  defp queue_cap_bytes(%Impairment{rate_kbps: :infinity}), do: :infinity

  defp queue_cap_bytes(%Impairment{queue_ms: queue_ms} = impairment),
    do: max(8 * @mtu, round(bytes_per_ms(impairment) * queue_ms))
end
