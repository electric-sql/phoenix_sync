defmodule PhoenixSyncExample.CarsTest do
  use PhoenixSyncExample.DataCase

  alias PhoenixSyncExample.Cars
  alias PhoenixSyncExample.Cars.Model

  import PhoenixSyncExample.CarsFixtures

  describe "cars" do
    @invalid_attrs %{name: nil, cost: nil}

    test "list_cars/0 returns all cars" do
      car = car_fixture()
      assert Cars.list_cars() == [car]
    end

    test "get_car!/1 returns the car with given id" do
      car = car_fixture()
      assert Cars.get_car!(car.id) == car
    end

    test "create_car/1 with valid data creates a car" do
      make = make_fixture()
      valid_attrs = %{name: "some name", cost: 42, make_id: make.id}

      assert {:ok, %Model{} = car} = Cars.create_car(valid_attrs)
      assert car.name == "some name"
      assert car.cost == 42
    end

    test "create_car/1 with invalid data returns error changeset" do
      assert {:error, %Ecto.Changeset{}} = Cars.create_car(@invalid_attrs)
    end

    test "update_car/2 with valid data updates the car" do
      car = car_fixture()
      update_attrs = %{name: "some updated name", cost: 43}

      assert {:ok, %Model{} = car} = Cars.update_car(car, update_attrs)
      assert car.name == "some updated name"
      assert car.cost == 43
    end

    test "update_car/2 with invalid data returns error changeset" do
      car = car_fixture()
      assert {:error, %Ecto.Changeset{}} = Cars.update_car(car, @invalid_attrs)
      assert car == Cars.get_car!(car.id)
    end

    test "delete_car/1 deletes the car" do
      car = car_fixture()
      assert {:ok, %Model{}} = Cars.delete_car(car)
      assert_raise Ecto.NoResultsError, fn -> Cars.get_car!(car.id) end
    end

    test "change_car/1 returns a car changeset" do
      car = car_fixture()
      assert %Ecto.Changeset{} = Cars.change_car(car)
    end
  end

  describe "sync" do
    test "works with the sandbox" do
      parent = self()
      client = Phoenix.Sync.client!()

      start_supervised!(
        {Task,
         fn ->
           for msg <- Electric.Client.stream(client, Model, replica: :full),
               do: send(parent, {:change, msg})
         end}
      )

      %{id: id} = car_fixture(%{name: "Bunny"})

      assert_receive {:change,
                      %Electric.Client.Message.ChangeMessage{
                        value: %Model{id: ^id, name: "Bunny"},
                        headers: %{operation: :insert}
                      }},
                     1000
    end
  end
end
