defmodule Phoenix.Sync.LiveViewTest.StreamLive do
  use Phoenix.LiveView

  def run(lv, func) do
    GenServer.call(lv.pid, {:run, func})
  end

  def render(assigns) do
    ~H"""
    <div id="users" phx-update="stream">
      <div :for={{id, user} <- @streams.users} id={id} data-count={@count}>
        <%= user.name %>
        <button phx-click="delete" phx-value-id={id}>delete</button>
        <button phx-click="update" phx-value-id={id}>update</button>
        <button phx-click="move-to-first" phx-value-id={id}>make first</button>
        <button phx-click="move-to-last" phx-value-id={id}>make last</button>
        <button phx-click="move" phx-value-id={id} phx-value-name="moved" phx-value-at="1">
          move
        </button>
        <button phx-click={Phoenix.LiveView.JS.hide(to: "##{id}")}>JS Hide</button>
      </div>
    </div>

    <button phx-click="reset-users">Reset users</button>
    """
  end

  def mount(_params, _session, socket) do
    client =
      get_in(socket.private.connect_info.private, [:electric_client]) ||
        raise "missing client configuration"

    parent =
      get_in(socket.private.connect_info.private, [:test_pid]) ||
        raise "missing parent pid configuration"

    {:ok,
     socket
     |> assign(:count, 0)
     |> assign(:test_pid, parent)
     |> Phoenix.Sync.LiveView.sync_stream(:users, Support.User, client: client)}
  end

  def handle_info({:sync, event}, socket) do
    # send message to test pid, just for sync
    send(socket.assigns.test_pid, {:sync, event})
    {:noreply, Phoenix.Sync.LiveView.sync_stream_update(socket, event)}
  end

  def handle_info(:ping, socket) do
    {:noreply, update(socket, :count, &(&1 + 1))}
  end

  def handle_event("admin-move-to-last", %{"id" => "admins-" <> id = dom_id}, socket) do
    user = user(id, "updated")

    {:noreply,
     socket
     |> stream_delete_by_dom_id(:admins, dom_id)
     |> stream_insert(:admins, user, at: -1)}
  end

  def handle_event("consume-stream-invalid", _, socket) do
    {:noreply, assign(socket, :invalid_consume, true)}
  end

  def handle_call({:run, func}, _, socket), do: func.(socket)

  defp user(id, name) do
    %{id: id, name: name}
  end
end

defmodule Phoenix.LiveViewTest.StreamComponent do
  use Phoenix.LiveComponent

  def run(lv, func) do
    GenServer.call(lv.pid, {:run, func})
  end

  def render(assigns) do
    ~H"""
    <div id="c_users" phx-update="stream">
      <div :for={{id, user} <- @streams.c_users} id={id}>
        <%= user.name %>
        <button phx-click="delete" phx-value-id={id} phx-target={@myself}>delete</button>
        <button phx-click="update" phx-value-id={id} phx-target={@myself}>update</button>
        <button phx-click="move-to-first" phx-value-id={id} phx-target={@myself}>make first</button>
        <button phx-click="move-to-last" phx-value-id={id} phx-target={@myself}>make last</button>
      </div>
    </div>
    """
  end

  def update(%{reset: {stream, collection}}, socket) do
    {:ok, stream(socket, stream, collection, reset: true)}
  end

  def update(%{send_assigns_to: test_pid}, socket) when is_pid(test_pid) do
    send(test_pid, {:assigns, socket.assigns})
    {:ok, socket}
  end

  def update(_assigns, socket) do
    users = [user(1, "chris"), user(2, "callan")]
    {:ok, stream(socket, :c_users, users)}
  end

  def handle_event("reset", %{}, socket) do
    {:noreply, stream(socket, :c_users, [], reset: true)}
  end

  def handle_event("delete", %{"id" => dom_id}, socket) do
    {:noreply, stream_delete_by_dom_id(socket, :c_users, dom_id)}
  end

  def handle_event("update", %{"id" => "c_users-" <> id}, socket) do
    {:noreply, stream_insert(socket, :c_users, user(id, "updated"))}
  end

  def handle_event("move-to-first", %{"id" => "c_users-" <> id}, socket) do
    {:noreply,
     socket
     |> stream_delete_by_dom_id(:c_users, "c_users-" <> id)
     |> stream_insert(:c_users, user(id, "updated"), at: 0)}
  end

  def handle_event("move-to-last", %{"id" => "c_users-" <> id = dom_id}, socket) do
    user = user(id, "updated")

    {:noreply,
     socket
     |> stream_delete_by_dom_id(:c_users, dom_id)
     |> stream_insert(:c_users, user, at: -1)}
  end

  defp user(id, name) do
    %{id: id, name: name}
  end
end

defmodule Phoenix.Sync.LiveViewTest.StreamLiveWithComponent do
  use Phoenix.LiveView

  def run(lv, func) do
    GenServer.call(lv.pid, {:run, func})
  end

  def render(assigns) do
    ~H"""
    <div>
      <.live_component id="my_component" client={@client} test_pid={@test_pid} module={Phoenix.Sync.LiveViewTest.StreamLiveComponent} />
    </div>
    """
  end

  def mount(_params, _session, socket) do
    client =
      get_in(socket.private.connect_info.private, [:electric_client]) ||
        raise "missing client configuration"

    parent =
      get_in(socket.private.connect_info.private, [:test_pid]) ||
        raise "missing parent pid configuration"

    {:ok,
     socket
     |> assign(:count, 0)
     |> assign(:test_pid, parent)
     |> assign(:client, client)}
  end

  def handle_info({:sync, event}, socket) do
    {:noreply, Phoenix.Sync.LiveView.sync_stream_update(socket, event)}
  end
end

defmodule Phoenix.Sync.LiveViewTest.StreamLiveComponent do
  use Phoenix.LiveComponent

  def render(assigns) do
    ~H"""
    <div id="users" phx-update="stream">
      <div :for={{id, user} <- @streams.users} id={id}>
        <%= user.name %>
      </div>
    </div>
    """
  end

  def mount(socket) do
    {:ok, socket}
  end

  def update(%{sync: {:users, :live}}, socket) do
    send(socket.assigns.test_pid, {:sync, :users, :live})
    {:ok, socket}
  end

  def update(%{sync: event}, socket) do
    send(socket.assigns.test_pid, {:sync, event})
    {:ok, Phoenix.Sync.LiveView.sync_stream_update(socket, event)}
  end

  def update(assigns, socket) do
    {:ok,
     socket
     |> assign(:client, assigns.client)
     |> assign(:test_pid, assigns.test_pid)
     |> Phoenix.Sync.LiveView.sync_stream(:users, Support.User, client: assigns.client)}
  end
end

defmodule Phoenix.Sync.LiveViewTest.StreamSandbox do
  use Phoenix.LiveView

  def run(lv, func) do
    GenServer.call(lv.pid, {:run, func})
  end

  def render(assigns) do
    ~H"""
    <div id="todos" phx-update="stream">
      <div :for={{id, todo} <- @streams.todos} id={id}>
        [<%= if(todo.completed, do: "X", else: " ") %>] <%= todo.title %>
      </div>
    </div>
    """
  end

  def mount(_params, _session, socket) do
    parent =
      get_in(socket.private.connect_info.private, [:test_pid]) ||
        raise "missing parent pid configuration"

    {:ok,
     socket
     |> assign(:test_pid, parent)
     |> Phoenix.Sync.LiveView.sync_stream(:todos, Support.Todo)}
  end

  def handle_info({:sync, event}, socket) do
    # send message to test pid, just for sync
    send(socket.assigns.test_pid, {:sync, event})
    {:noreply, Phoenix.Sync.LiveView.sync_stream_update(socket, event)}
  end
end

defmodule Phoenix.Sync.LiveViewTest.StreamLiveKeyword do
  use Phoenix.LiveView

  def run(lv, func) do
    GenServer.call(lv.pid, {:run, func})
  end

  def render(assigns) do
    ~H"""
    <div id="users" phx-update="stream">
      <div :for={{id, user} <- @streams.users} id={id}>
        <%= user["name"] %>
      </div>
    </div>
    """
  end

  def mount(_params, _session, socket) do
    client =
      get_in(socket.private.connect_info.private, [:electric_client]) ||
        raise "missing client configuration"

    parent =
      get_in(socket.private.connect_info.private, [:test_pid]) ||
        raise "missing parent pid configuration"

    {:ok,
     socket
     |> assign(:test_pid, parent)
     |> Phoenix.Sync.LiveView.sync_stream(:users, [table: "users", where: "name != ''"],
       client: client
     )}
  end

  def handle_info({:sync, event}, socket) do
    # send message to test pid, just for sync
    send(socket.assigns.test_pid, {:sync, event})
    {:noreply, Phoenix.Sync.LiveView.sync_stream_update(socket, event)}
  end
end
