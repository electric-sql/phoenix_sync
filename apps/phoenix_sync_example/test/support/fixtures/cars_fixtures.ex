defmodule PhoenixSyncExample.CarsFixtures do
  @moduledoc """
  This module defines test helpers for creating
  entities via the `PhoenixSyncExample.Cars` context.
  """

  def make_fixture(attrs \\ %{}) do
    {:ok, make} =
      attrs
      |> Enum.into(%{name: "Blue"})
      |> PhoenixSyncExample.Cars.create_make()

    make
  end

  @doc """
  Generate a car.
  """
  def car_fixture(attrs \\ %{}) do
    make = Map.get_lazy(attrs, :make, &make_fixture/0)

    {:ok, car} =
      attrs
      |> Enum.into(%{cost: 42, name: "Phantom", make_id: make.id})
      |> PhoenixSyncExample.Cars.create_car()

    car
  end
end
