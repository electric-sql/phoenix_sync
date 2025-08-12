defmodule PhoenixSyncExampleWeb.CarControllerTest do
  use PhoenixSyncExampleWeb.ConnCase

  import PhoenixSyncExample.CarsFixtures

  alias PhoenixSyncExample.Cars.Model

  @create_attrs %{
    name: "some name",
    cost: 42,
    # see priv/repo/seeds.exs for the make_id
    make_id: "52e372d3-cb45-401c-8d2a-6e898e99cea3"
  }
  @update_attrs %{
    name: "some updated name",
    cost: 43
  }
  @invalid_attrs %{name: nil, cost: nil}

  setup %{conn: conn} do
    {:ok, conn: put_req_header(conn, "accept", "application/json")}
  end

  describe "index" do
    test "lists all cars", %{conn: conn} do
      conn = get(conn, ~p"/api/cars")
      assert json_response(conn, 200)["data"] == []
    end
  end

  describe "create car" do
    test "renders car when data is valid", %{conn: conn} do
      conn = post(conn, ~p"/api/cars", car: @create_attrs)
      assert %{"id" => id} = json_response(conn, 201)["data"]

      conn = get(conn, ~p"/api/cars/#{id}")

      assert %{
               "id" => ^id,
               "cost" => 42,
               "name" => "some name"
             } = json_response(conn, 200)["data"]
    end

    test "renders errors when data is invalid", %{conn: conn} do
      conn = post(conn, ~p"/api/cars", car: @invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end
  end

  describe "update car" do
    setup [:create_car]

    test "renders car when data is valid", %{conn: conn, car: %Model{id: id} = car} do
      conn = put(conn, ~p"/api/cars/#{car}", car: @update_attrs)
      assert %{"id" => ^id} = json_response(conn, 200)["data"]

      conn = get(conn, ~p"/api/cars/#{id}")

      assert %{
               "id" => ^id,
               "cost" => 43,
               "name" => "some updated name"
             } = json_response(conn, 200)["data"]
    end

    test "renders errors when data is invalid", %{conn: conn, car: car} do
      conn = put(conn, ~p"/api/cars/#{car}", car: @invalid_attrs)
      assert json_response(conn, 422)["errors"] != %{}
    end
  end

  describe "delete car" do
    setup [:create_car]

    test "deletes chosen car", %{conn: conn, car: car} do
      conn = delete(conn, ~p"/api/cars/#{car}")
      assert response(conn, 204)

      assert_error_sent 404, fn ->
        get(conn, ~p"/api/cars/#{car}")
      end
    end
  end

  defp create_car(_) do
    car = car_fixture()
    %{car: car}
  end
end
