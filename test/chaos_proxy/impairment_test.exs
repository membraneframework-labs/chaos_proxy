defmodule ChaosProxy.ImpairmentTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ChaosProxy.Impairment

  property "gilbert/1 loses loss_pct in spells of loss_burst, or longer where those cannot" do
    check all pct <- integer(1..99),
              burst <- one_of([integer(1..50), float(min: 1.0, max: 50.0)]) do
      {p, r} = Impairment.gilbert(%Impairment{loss_pct: pct, loss_burst: burst})

      assert p > 0 and p <= 1 and r > 0 and r <= 1
      assert_in_delta 100 * p / (p + r), pct, 1.0e-9

      if pct <= 100 * burst / (burst + 1),
        do: assert_in_delta(1 / r, burst, 1.0e-9),
        else: assert_in_delta(p, 1.0, 1.0e-9)
    end
  end

  describe "split/1" do
    test "a bare impairment is the downlink, its delay, jitter and blackout mirrored on the uplink" do
      down = %Impairment{
        rate_kbps: 700,
        queue_ms: 50,
        loss_pct: 2,
        loss_burst: 3,
        delay_ms: 10,
        jitter_ms: 4,
        blackout?: true
      }

      assert Impairment.split(down) == %{
               down: down,
               up: %Impairment{delay_ms: 10, jitter_ms: 4, blackout?: true}
             }
    end

    test "a keyword list sets the directions it names and no other" do
      thin = %Impairment{rate_kbps: 500}

      assert Impairment.split(up: thin) == %{up: thin}
      assert Impairment.split(down: thin) == %{down: thin}
      assert Impairment.split(up: thin, down: %Impairment{}) == %{up: thin, down: %Impairment{}}
      assert Impairment.split([]) == %{}
    end

    test "takes any rate above 0, and no limit" do
      for rate <- [1, 0.5, 1.0e-3, 100_000, :infinity] do
        impairment = %Impairment{rate_kbps: rate}
        assert %{down: ^impairment} = Impairment.split(impairment)
      end
    end

    test "refuses bursts of less than a packet" do
      for burst <- [0, 0.5, -1, nil] do
        assert_raise ArgumentError, ~r/loss_burst must be 1 or more/, fn ->
          Impairment.split(%Impairment{loss_pct: 5, loss_burst: burst})
        end
      end
    end

    test "refuses a rate that lets nothing through" do
      for rate <- [0, 0.0, -0.0, -5, -5.0] do
        assert_raise ArgumentError, ~r/rate_kbps must be above 0/, fn ->
          Impairment.split(%Impairment{rate_kbps: rate})
        end
      end
    end

    test "refuses a keyword list with any impairment it would refuse bare" do
      assert_raise ArgumentError, ~r/rate_kbps must be above 0/, fn ->
        Impairment.split(down: %Impairment{}, up: %Impairment{rate_kbps: 0})
      end
    end
  end
end
