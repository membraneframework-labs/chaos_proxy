defmodule ChaosProxy.ImpairmentTest do
  use ExUnit.Case, async: true

  alias ChaosProxy.Impairment

  doctest Impairment

  test "is a transparent link by default" do
    assert %Impairment{blackout?: false, loss_pct: 0, rate_kbps: :infinity, delay_ms: 0} =
             %Impairment{}
  end

  describe "gilbert/1" do
    test "loses loss_pct of the packets in spells of loss_burst" do
      for pct <- [1, 5, 20, 50], burst <- [2, 5, 20, 2.5] do
        {p, r} = Impairment.gilbert(%Impairment{loss_pct: pct, loss_burst: burst})

        assert_in_delta 1 / r, burst, 1.0e-9
        assert_in_delta 100 * p / (p + r), pct, 1.0e-9
      end
    end

    test "has longer spells where loss_burst cannot lose loss_pct" do
      for {pct, burst, spell} <- [{80, 2, 4.0}, {90, 3, 9.0}, {75, 3, 3.0}] do
        {p, r} = Impairment.gilbert(%Impairment{loss_pct: pct, loss_burst: burst})

        assert_in_delta p, 1.0, 1.0e-9
        assert_in_delta 1 / r, spell, 1.0e-9
        assert_in_delta 100 * p / (p + r), pct, 1.0e-9
      end
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
        assert %{up: ^impairment} = Impairment.split(up: impairment)
      end
    end

    test "refuses bursts of less than a packet" do
      for burst <- [0, 0.5, -1, nil] do
        impairment = %Impairment{loss_pct: 5, loss_burst: burst}

        assert_raise ArgumentError, ~r/loss_burst must be 1 or more/, fn ->
          Impairment.split(impairment)
        end

        assert_raise ArgumentError, ~r/loss_burst must be 1 or more/, fn ->
          Impairment.split(up: impairment)
        end
      end
    end

    test "refuses a rate that lets nothing through" do
      for rate <- [0, 0.0, -0.0, -5, -5.0] do
        impairment = %Impairment{rate_kbps: rate}

        assert_raise ArgumentError, ~r/rate_kbps must be above 0/, fn ->
          Impairment.split(impairment)
        end

        assert_raise ArgumentError, ~r/rate_kbps must be above 0/, fn ->
          Impairment.split(down: %Impairment{}, up: impairment)
        end
      end
    end
  end
end
