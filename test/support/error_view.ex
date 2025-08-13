defmodule Phoenix.ErrorView do
  def render("500.html", %{reason: exception}) do
    Exception.message(exception)
  end
end
