if Code.ensure_loaded?(Phoenix.Component) do
  defmodule Phoenix.Sync.LiveView do
    @moduledoc """
    Swap out `Phoenix.LiveView.stream/3` for `Phoenix.Sync.LiveView.sync_stream/4` to
    automatically keep a LiveView up-to-date with the state of your Postgres database:

    ```elixir
    defmodule MyWeb.MyLive do
      use Phoenix.LiveView
      import Phoenix.Sync.LiveView

      def mount(_params, _session, socket) do
        {:ok, sync_stream(socket, :todos, Todos.Todo)}
      end

      def handle_info({:sync, event}, socket) do
        {:noreply, sync_stream_update(socket, event)}
      end
    end
    ```
    """

    use Phoenix.Component

    alias Electric.Client.Message

    require Record

    Record.defrecordp(:event, :"$electric_event", [:name, :operation, :item, opts: []])
    Record.defrecordp(:component_event, :"$electric_component_event", [:component, :event])

    @options NimbleOptions.new!(client: [type: {:struct, Electric.Client}])

    @opaque root_event() ::
              record(:event,
                name: :atom | String.t(),
                operation: :atom,
                item: Electric.Client.message()
              )
    @opaque component_event() ::
              record(:component_event,
                component: term(),
                event: root_event()
              )
    @opaque replication_event() :: root_event() | component_event()
    @type state_event() :: {atom(), :loaded} | {atom(), :live}

    @type event() :: replication_event() | state_event()

    @type stream_option() ::
            {:at, integer()}
            | {:limit, pos_integer()}
            | {:reset, boolean()}
            | unquote(NimbleOptions.option_typespec(@options))

    @type stream_options() :: [stream_option()]

    @doc ~S"""
    Maintains a LiveView stream from the given Ecto query.

    - `name` The name to use for the LiveView stream.
    - `query` An [`Ecto`](`Ecto`) query that represents the data to stream from the database.

    For example:

        def mount(_params, _session, socket) do
          socket =
            Phoenix.Sync.LiveView.sync_stream(
              socket,
              :admins,
              from(u in Users, where: u.admin == true)
            )
          {:ok, socket}
        end

    This will subscribe to the configured Electric server and keep the list of
    `:admins` in sync with the database via a `Phoenix.LiveView` stream.

    Updates will be delivered to the view via messages to the LiveView process.

    To handle these you need to add a `handle_info/2` implementation that receives these:

        def handle_info({:sync, event}, socket) do
          {:noreply, Phoenix.Sync.LiveView.sync_stream_update(socket, event)}
        end

    See the docs for `Phoenix.LiveView.stream/4` for details on using LiveView streams.

    ## Lifecycle Events

    Most `{:sync, event}` messages are opaque and should be passed directly
    to the `sync_stream_update/3` function, but there are two events that are
    outside Electric's replication protocol and designed to be useful in the
    LiveView component.

    - `{:sync, {stream_name, :loaded}}` - sent when the Electric event stream has passed
      from initial state to update mode.

      This event is useful to show the stream component after
      the initial sync. Because of the streaming nature of Electric Shapes, the
      intitial sync can cause flickering as items are added, removed and updated.

      E.g.:

          # in the LiveView component
          def handle_info({:sync, {_name, :live}}, socket) do
            {:noreply, assign(socket, :show_stream, true)}
          end

          # in the template
          <div phx-update="stream" class={unless(@show_stream, do: "opacity-0")}>
            <div :for={{id, item} <- @streams.items} id={id}>
              <%= item.value %>
            </div>
          </div>

    - `{:sync, {stream_name, :live}}` - sent when the Electric stream is in
      `live` mode, that is the initial state has loaded and the client is
      up-to-date with the database and is long-polling for new events from the
      Electric server.

    If your app doesn't need this extra information, then you can ignore them and
    just have a catch-all callback:

        def handle_info({:sync, event}, socket) do
          {:noreply, Phoenix.Sync.LiveView.sync_stream_update(socket, event)}
        end

    `Phoenix.Sync.LiveView.sync_stream_update` will just ignore the
    lifecycle events.

    ## Sub-components

    If you register your Electric stream in a sub-component you will still
    receive Electric messages in the LiveView's root/parent process.

    `Phoenix.Sync` handles this for you by encapsulating component messages
    so it can correctly forward on the event to the component.

    So in the parent `LiveView` process you handle the `:sync` messages as
    above:

        defmodule MyLiveView do
          use Phoenix.LiveView

          def render(assigns) do
            ~H\"""
            <div>
              <.live_component id="my_component" module={MyComponent} />
            </div>
            \"""
          end

          # We setup the Electric sync_stream in the component but update messages will
          # be sent to the parent process.
          def handle_info({:sync, event}, socket) do
            {:noreply, Phoenix.Sync.LiveView.sync_stream_update(socket, event)}
          end
        end

    In the component you must handle these events in the
    `c:Phoenix.LiveComponent.update/2` callback:

        defmodule MyComponent do
          use Phoenix.LiveComponent

          def render(assigns) do
            ~H\"""
            <div id="users" phx-update="stream">
              <div :for={{id, user} <- @streams.users} id={id}>
                <%= user.name %>
              </div>
            </div>
            \"""
          end

          # Equivalent to the `handle_info({:sync, {stream_name, :live}}, socket)` callback
          # in the parent LiveView.
          def update(%{sync: {_stream_name, :live}}, socket) do
            {:ok, socket}
          end

          # Equivalent to the `handle_info({:sync, event}, socket)` callback
          # in the parent LiveView.
          def update(%{sync: event}, socket) do
            {:ok, Phoenix.Sync.LiveView.sync_stream_update(socket, event)}
          end

          def update(assigns, socket) do
            {:ok, Phoenix.Sync.LiveView.sync_stream(socket, :users, User)}
          end
        end
    """
    @spec sync_stream(
            socket :: Phoenix.LiveView.Socket.t(),
            name :: atom() | String.t(),
            query :: Ecto.Queryable.t(),
            opts :: stream_options()
          ) :: Phoenix.LiveView.Socket.t()
    def sync_stream(socket, name, query, opts \\ []) do
      {electric_opts, stream_opts} = Keyword.split(opts, [:client])

      component =
        case socket.assigns do
          %{myself: %Phoenix.LiveComponent.CID{} = component} -> component
          _ -> nil
        end

      if Phoenix.LiveView.connected?(socket) do
        client =
          Keyword.get_lazy(electric_opts, :client, fn ->
            get_in(socket.private[:connect_info].private[:electric_client]) ||
              Phoenix.Sync.client!()
          end)

        Phoenix.LiveView.stream(
          socket,
          name,
          client_live_stream(client, name, query, component),
          stream_opts
        )
      else
        Phoenix.LiveView.stream(socket, name, [], stream_opts)
      end
    end

    @doc """
    Handle Electric events within a LiveView.

        def handle_info({:sync, event}, socket) do
          {:noreply, Phoenix.Sync.LiveView.sync_stream_update(socket, event, at: 0)}
        end

    The `opts` are passed to the `Phoenix.LiveView.stream_insert/4` call.
    """
    @spec sync_stream_update(Phoenix.LiveView.Socket.t(), event(), Keyword.t()) ::
            Phoenix.LiveView.Socket.t()
    def sync_stream_update(socket, event, opts \\ [])

    def sync_stream_update(
          socket,
          component_event(component: component, event: {_name, status} = event),
          _opts
        )
        when status in [:live, :loaded] do
      Phoenix.LiveView.send_update(component, sync: event)
      socket
    end

    def sync_stream_update(socket, component_event(component: component, event: event), opts) do
      Phoenix.LiveView.send_update(component, sync: event(event, opts: opts))
      socket
    end

    def sync_stream_update(socket, {_name, :loaded}, _opts) do
      socket
    end

    def sync_stream_update(socket, {_name, :live}, _opts) do
      socket
    end

    def sync_stream_update(
          socket,
          event(operation: :insert, name: name, item: item, opts: event_opts),
          opts
        ) do
      Phoenix.LiveView.stream_insert(socket, name, item, Keyword.merge(event_opts, opts))
    end

    def sync_stream_update(socket, event(operation: :delete, name: name, item: item), _opts) do
      Phoenix.LiveView.stream_delete(socket, name, item)
    end

    defp client_live_stream(client, name, query, component) do
      pid = self()

      client
      |> Electric.Client.stream(query, live: false, replica: :full, errors: :stream)
      |> Stream.transform(
        fn -> {[], nil} end,
        &live_stream_message/2,
        &update_mode(&1, {client, name, query, pid, component})
      )
    end

    defp live_stream_message(
           %Message.ChangeMessage{headers: %{operation: :insert}, value: value},
           acc
         ) do
      {[value], acc}
    end

    defp live_stream_message(%Message.ChangeMessage{} = msg, {updates, resume}) do
      {[], {[msg | updates], resume}}
    end

    defp live_stream_message(%Message.ControlMessage{}, acc) do
      {[], acc}
    end

    defp live_stream_message(%Message.ResumeMessage{} = resume, {updates, nil}) do
      {[], {updates, resume}}
    end

    defp live_stream_message(%Electric.Client.Error{} = error, _acc) do
      {[], {error, nil}}
    end

    defp update_mode({%Electric.Client.Error{} = error, _resume}, _state) do
      raise error
    end

    defp update_mode({updates, resume}, {client, name, query, pid, component}) do
      # need to send every update as a separate message.

      for event <- updates |> Enum.reverse() |> Enum.map(&wrap_msg(&1, name, component)),
          do: send(pid, {:sync, event})

      send(pid, {:sync, wrap_event(component, {name, :loaded})})

      Task.start_link(fn ->
        client
        |> Electric.Client.stream(query, resume: resume, replica: :full)
        |> Stream.each(&send_live_event(&1, pid, name, component))
        |> Stream.run()
      end)
    end

    defp send_live_event(%Message.ChangeMessage{} = msg, pid, name, component) do
      send(pid, {:sync, wrap_msg(msg, name, component)})
    end

    defp send_live_event(%Message.ControlMessage{control: :up_to_date}, pid, name, component) do
      send(pid, {:sync, wrap_event(component, {name, :live})})
    end

    defp send_live_event(_msg, _pid, _name, _component) do
      nil
    end

    defp wrap_msg(%Message.ChangeMessage{headers: %{operation: operation}} = msg, name, component)
         when operation in [:insert, :update] do
      wrap_event(component, event(operation: :insert, name: name, item: msg.value))
    end

    defp wrap_msg(%Message.ChangeMessage{headers: %{operation: :delete}} = msg, name, component) do
      wrap_event(component, event(operation: :delete, name: name, item: msg.value))
    end

    defp wrap_event(nil, event) do
      event
    end

    defp wrap_event(component, event) do
      component_event(component: component, event: event)
    end

    _ = """
    Embed client configuration for a shape into your HTML.

        <Phoenix.Sync.LiveView.electric_client_configuration
          shape={MyApp.Todo}
          key="todo_shape_config"
        />

    This will put a `<script>` tag into your page setting
    `window.todo_shape_config` to the configuration needed to subscribe to
    changes to the `MyApp.Todo` table.

        <script>
          window.todo_shape_config = {"url":"https://localhost:3000/v1/shape/todos" /* , ... */}
        </script>

    If you include a `:script` slot then you have complete control over how the
    configuration is applied.

        <Phoenix.Sync.LiveView.electric_client_configuration shape={MyApp.Todo}>
          <:script :let={configuration}>
            const container = document.getElementById("root")
            const root = createRoot(container)
            root.render(
              React.createElement(
                MyApp, {
                  client_config: <%= configuration %>
                },
                null
              )
            );
          </:script>
        </Phoenix.Sync.LiveView.electric_client_configuration>

    The `configuration` variable in the `:script` block is the  JSON-encoded
    client configuration.

        <script>
          const container = document.getElementById("root")
          const root = createRoot(container)
          root.render(
            React.createElement(
              MyApp, {
                client_config: {"url":"https://localhost:3000/v1/shape/todos" /* , ... */}
              },
              null
            )
          );
        </script>
    """

    @doc false
    attr :shape, :any, required: true, doc: "The Ecto query (or schema module) to subscribe to"

    attr :key, :string,
      default: "electric_client_config",
      doc: "The key in the top-level `window` object to put the configuration object."

    attr :client, :any, doc: "Optional client. If not set defaults to `Phoenix.Sync.client!()`"

    slot :script,
      doc:
        "An optional inner block that allows you to override what you want to do with the configuration JSON"

    def electric_client_configuration(%{shape: shape} = assigns) do
      assigns = assign_new(assigns, :client, &Phoenix.Sync.client!/0)

      configuration = Phoenix.Sync.Gateway.configuration(shape, assigns.client)

      assigns =
        assign(
          assigns,
          :configuration,
          Phoenix.HTML.raw(Jason.encode!(configuration))
        )

      ~H"""
      <script>
        <%= if inner = render_slot(@script, @configuration) do %>
        <%= inner %>
        <% else %>
        window.<%= @key %> = <%= @configuration %>
        <% end %>
      </script>
      """
    end
  end
end
