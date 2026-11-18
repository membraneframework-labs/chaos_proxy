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

  describe "parse/1" do
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

      assert Impairment.parse(down) ==
               {:ok, %{down: down, up: %Impairment{delay_ms: 10, jitter_ms: 4, blackout?: true}}}
    end

    test "a keyword list sets the directions it names and no other" do
      thin = %Impairment{rate_kbps: 500}

      assert Impairment.parse(up: thin) == {:ok, %{up: thin}}
      assert Impairment.parse(down: thin) == {:ok, %{down: thin}}

      assert Impairment.parse(up: thin, down: %Impairment{}) ==
               {:ok, %{up: thin, down: %Impairment{}}}

      assert Impairment.parse([]) == {:ok, %{}}
    end

    test "takes any rate above 0, and no limit" do
      for rate <- [1, 0.5, 1.0e-3, 100_000, :infinity] do
        impairment = %Impairment{rate_kbps: rate}
        assert {:ok, %{down: ^impairment}} = Impairment.parse(impairment)
      end
    end

    test "refuses a field outside its type" do
      for {fields, message} <- [
            {[loss_burst: 0.5], ~r/:loss_burst must be 1 or more, got: 0.5/},
            {[loss_burst: nil], ~r/:loss_burst must be 1 or more/},
            {[rate_kbps: 0], ~r/:rate_kbps must be above 0 or :infinity, got: 0/},
            {[rate_kbps: -0.0], ~r/:rate_kbps must be above 0/},
            {[loss_pct: 101], ~r/:loss_pct must be an integer from 0 to 100/},
            {[loss_pct: 2.5], ~r/:loss_pct must be an integer from 0 to 100/},
            {[blackout?: nil], ~r/:blackout\? must be a boolean/},
            {[queue_ms: 0], ~r/:queue_ms must be a positive integer/},
            {[delay_ms: -1], ~r/:delay_ms must be a non-negative integer/},
            {[jitter_ms: 1.5], ~r/:jitter_ms must be a non-negative integer/}
          ] do
        assert {:error, reason} = Impairment.parse(struct!(Impairment, fields))
        assert reason =~ message
      end
    end

    test "refuses a keyword list with any impairment it would refuse bare, or another key" do
      assert {:error, reason} =
               Impairment.parse(down: %Impairment{}, up: %Impairment{rate_kbps: 0})

      assert reason =~ ~r/:rate_kbps must be above 0/

      assert {:error, reason} = Impairment.parse(dwon: %Impairment{})
      assert reason =~ ~r/expected \{:up \| :down, %ChaosProxy.Impairment\{\}\}, got: \{:dwon/

      assert {:error, reason} = Impairment.parse(%{down: %Impairment{}})
      assert reason =~ ~r/impairments must be/
    end
  end
end
