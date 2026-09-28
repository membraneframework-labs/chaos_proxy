defmodule ChaosProxy.ImpairmentTest do
  use ExUnit.Case, async: true

  alias ChaosProxy.Impairment

  test "is a transparent link by default" do
    assert %Impairment{blackout?: false, loss_pct: 0, rate_kbps: :infinity, delay_ms: 0} =
             %Impairment{}
  end

  describe "split/1" do
    test "a bare impairment is the downlink, its delay and blackout mirrored on the uplink" do
      down = %Impairment{
        rate_kbps: 700,
        queue_ms: 50,
        loss_pct: 2,
        delay_ms: 10,
        blackout?: true
      }

      assert Impairment.split(down) == %{
               down: down,
               up: %Impairment{delay_ms: 10, blackout?: true}
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
