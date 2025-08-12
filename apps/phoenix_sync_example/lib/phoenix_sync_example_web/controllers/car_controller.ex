defmodule PhoenixSyncExampleWeb.CarController do
  use PhoenixSyncExampleWeb, :controller

  alias PhoenixSyncExample.Cars
  alias PhoenixSyncExample.Cars.Model

  action_fallback PhoenixSyncExampleWeb.FallbackController

  def index(conn, _params) do
    cars = Cars.list_cars()
    render(conn, :index, cars: cars)
  end

  def create(conn, %{"car" => car_params}) do
    with {:ok, %Model{} = car} <- Cars.create_car(car_params) do
      conn
      |> put_status(:created)
      |> put_resp_header("location", ~p"/api/cars/#{car}")
      |> render(:show, car: car)
    end
  end

  def show(conn, %{"id" => id}) do
    car = Cars.get_car!(id)
    render(conn, :show, car: car)
  end

  def update(conn, %{"id" => id, "car" => car_params}) do
    car = Cars.get_car!(id)

    with {:ok, %Model{} = car} <- Cars.update_car(car, car_params) do
      render(conn, :show, car: car)
    end
  end

  def delete(conn, %{"id" => id}) do
    car = Cars.get_car!(id)

    with {:ok, %Model{}} <- Cars.delete_car(car) do
      send_resp(conn, :no_content, "")
    end
  end
end
