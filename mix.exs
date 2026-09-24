defmodule ChaosProxy.MixProject do
  use Mix.Project

  def project do
    [
      app: :chaos_proxy,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: false,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      # Only so `ChaosProxy.Impairment` can derive its encoder when the user has Jason.
      {:jason, "~> 1.4", optional: true}
    ]
  end
end
