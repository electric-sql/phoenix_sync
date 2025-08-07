defmodule PhoenixSyncExampleWeb.CarJSON do
  alias PhoenixSyncExample.Cars.Model

  @doc """
  Renders a list of cars.
  """
  def index(%{cars: cars}) do
    %{data: for(car <- cars, do: data(car))}
  end

  @doc """
  Renders a single car.
  """
  def show(%{car: car}) do
    %{data: data(car)}
  end

  defp data(%Model{} = car) do
    %{
      id: car.id,
      name: car.name,
      cost: car.cost
    }
  end
end
