# Phoenix.Sync

Real-time sync for Postgres-backed [Phoenix](https://www.phoenixframework.org/) applications.

<p>
  <a href="https://hexdocs.pm/phoenix_sync" target="_blank">
    <picture>
      <img alt="Phoenix sync illustration"
          src="https://github.com/electric-sql/phoenix_sync/raw/main/docs/phoenix-sync.png"
      />
    </picture>
  </a>
</p>

[![Hex.pm](https://img.shields.io/hexpm/v/phoenix_sync.svg)](https://hex.pm/packages/phoenix_sync)
[![Documentation](https://img.shields.io/badge/docs-hexdocs-green)](https://hexdocs.pm/phoenix_sync)
[![License](https://img.shields.io/badge/license-Apache_2.0-green)](./LICENSE)
[![Status](https://img.shields.io/badge/status-beta-orange)](https://github.com/electric-sql/phoenix_sync)
[![Discord](https://img.shields.io/discord/933657521581858818?color=5969EA&label=discord)](https://discord.electric-sql.com)

Documentation is available at [hexdocs.pm/phoenix_sync](https://hexdocs.pm/phoenix_sync).

## Build real-time apps on sync

Phoenix.Sync is a library that adds real-time sync to Postgres-backed [Phoenix](https://www.phoenixframework.org/) applications. Use it to sync data into both LiveView and front-end web and mobile applications.

- integrates with `Plug` and `Phoenix.{Controller, LiveView, Router, Stream}`
- uses [ElectricSQL](https://electric-sql.com) for core sync, fan-out and data delivery
- maps `Ecto.Query`s to [Shapes](https://electric-sql.com/docs/guides/shapes) for partial replication

There are four key APIs for [read-path sync](#read-path-sync) out of Postgres:

- `Phoenix.Sync.Client.stream/2` for low level usage in Elixir
- `Phoenix.Sync.LiveView.sync_stream/4` to sync into a LiveView
- `Phoenix.Sync.Router.sync/2` macro to expose a shape in your Router
- `Phoenix.Sync.Controller.sync_render/3` to return shapes from a Controller

And a `Phoenix.Sync.Writer` module for handling [write-path sync](#write-path-sync) back into Postgres.

## Read-path sync

### Low level usage in Elixir

Use `Phoenix.Sync.Client.stream/2` to convert an `Ecto.Query` into an Elixir `Stream`:

```elixir
stream = Phoenix.Sync.Client.stream(Todos.Todo)

stream =
  Ecto.Query.from(t in Todos.Todo, where: t.completed == false)
  |> Phoenix.Sync.Client.stream()
```

### Sync into a LiveView stream

Swap out `Phoenix.LiveView.stream/3` for `Phoenix.Sync.LiveView.sync_stream/4` to automatically keep a LiveView up-to-date with the state of your Postgres database:

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

LiveView takes care of automatically keeping the front-end up-to-date with the assigned stream. What Phoenix.Sync does is automatically keep the _stream_ up-to-date with the state of the database.

This means you can build fully end-to-end real-time multi-user applications without writing Javascript _and_ without worrying about message delivery, reconnections, cache invalidation or polling the database for changes.

### Sync shapes through your Router

Use the `Phoenix.Sync.Router.sync/2` macro to expose statically (compile-time) defined shapes in your Router:

```elixir
defmodule MyWeb.Router do
  use Phoenix.Router
  import Phoenix.Sync.Router

  pipeline :sync do
    plug :my_auth
  end

  scope "/shapes" do
    pipe_through :sync

    sync "/todos", Todos.Todo
  end
end
```

Because the shapes are exposed through your Router, the client connects through your existing Plug middleware. This allows you to do real-time sync straight out of Postgres _without_ having to translate your auth logic into complex/fragile database rules.

### Sync dynamic shapes from a Controller

Sync shapes from any standard Controller using the `Phoenix.Sync.Controller.sync_render/3` view function:

```elixir
defmodule Phoenix.Sync.LiveViewTest.TodoController do
  use Phoenix.Controller
  import Phoenix.Sync.Controller
  import Ecto.Query, only: [from: 2]

  def show(conn, %{"done" => done} = params) do
    sync_render(conn, params, from(t in Todos.Todo, where: t.done == ^done))
  end

  def show_mine(%{assigns: %{current_user: user_id}} = conn, params) do
    sync_render(conn, params, from(t in Todos.Todo, where: t.owner_id == ^user_id))
  end
end
```

This allows you to define and personalise the shape definition at runtime using the session and request.

### Consume shapes in the frontend

You can sync _into_ any client in any language that [speaks HTTP and JSON](https://electric-sql.com/docs/api/http).

For example, using the Electric [TypeScript client](https://electric-sql.com/docs/api/clients/typescript):

```typescript
import { Shape, ShapeStream } from "@electric-sql/client";

const stream = new ShapeStream({
  url: `/shapes/todos`,
});
const shape = new Shape(stream);

// The callback runs every time the data changes.
shape.subscribe((data) => console.log(data));
```

Or binding a shape to a component using the [React bindings](https://electric-sql.com/docs/integrations/react):

```tsx
import { useShape } from "@electric-sql/react";

const MyComponent = () => {
  const { data } = useShape({
    url: `shapes/todos`,
  });

  return <List todos={data} />;
};
```

See the Electric [demos](https://electric-sql.com/demos) and [documentation](https://electric-sql.com/docs) for more client-side usage examples.

## Write-path sync

The `Phoenix.Sync.Writer` module allows you to ingest batches of writes from the client.

The idea is that the front-end can batch up [local optimistic writes](https://electric-sql.com/docs/guides/writes). For example using a library like [@TanStack/db](https://github.com/TanStack/db) or by [monitoring changes to a local embedded database](https://electric-sql.com/docs/guides/writes#through-the-db).

These changes can be POSTed to a `Phoenix.Controller`, which then constructs a `Phoenix.Sync.Writer` instance. The writer instance authorizes and validates the writes before applying them to the database. Under the hood this uses `Ecto.Multi`, to ensure that transactions (batches of writes) are applied atomically.

For example, the controller below handles local writes made to a project management app. It constructs a writer instance and pipes it through a series of `Phoenix.Sync.Writer.allow/3` calls. These register functions against `Ecto.Schema`s (in this case `Projects.Project` and `Projects.Issue`):

```elixir
defmodule MutationController do
  use Phoenix.Controller, formats: [:json]

  alias Phoenix.Sync.Writer
  alias Phoenix.Sync.Writer.Format

  def mutate(conn, %{"transaction" => transaction} = _params) do
    user_id = conn.assigns.user_id

    {:ok, txid, _changes} =
      Phoenix.Sync.Writer.new()
      |> Phoenix.Sync.Writer.allow(
        Projects.Project,
        check: reject_invalid_params/2,
        load: &Projects.load_for_user(&1, user_id),
        validate: &Projects.Project.changeset/2
      )
      |> Phoenix.Sync.Writer.allow(
        Projects.Issue,
        # Use the sensible defaults:
        # validate: Projects.Issue.changeset/2
        # etc.
      )
      |> Phoenix.Sync.Writer.apply(transaction, Repo, format: Format.TanstackDB)

    render(conn, :mutations, txid: txid)
  end
end
```

This facilitates incrementally adding bi-directional sync support to a Phoenix application, re-using your existing auth and schema/validation logic.

See the `Phoenix.Sync.Writer` module docs for more information.

## Installation and configuration

`Phoenix.Sync` can be used in two modes:

1. `:embedded` where Electric is included as an application dependency and Phoenix.Sync consumes data internally using Elixir APIs
2. `:http` where Electric does _not_ need to be included as an application dependency and Phoenix.Sync consumes data from an external Electric service using it's [HTTP API](https://electric-sql.com/docs/api/http)

### Embedded mode

In `:embedded` mode, Electric must be included an application dependency but does not expose an HTTP API (internally or externally). Messages are streamed internally between Electric and Phoenix.Sync using Elixir function APIs. The only HTTP API for sync is that exposed via your Phoenix Router using the `sync/2` macro and `sync_render/3` function.

Example config:

```elixir
# mix.exs
defp deps do
  [
    {:electric, ">= 1.0.0-beta.20"},
    {:phoenix_sync, "~> 0.3"}
  ]
end

# config/config.exs
config :phoenix_sync,
  env: config_env(),
  mode: :embedded,
  repo: MyApp.Repo

# application.ex
children = [
  MyApp.Repo,
  # ...
  {MyApp.Endpoint, phoenix_sync: Phoenix.Sync.plug_opts()}
]
```

### HTTP

In `:http` mode, Electric does not need to be included as an application dependency. Instead, Phoenix.Sync consumes data from an external Electric service over HTTP.

```elixir
# mix.exs
defp deps do
  [
    {:phoenix_sync, "~> 0.3"}
  ]
end

# config/config.exs
config :phoenix_sync,
  env: config_env(),
  mode: :http,
  url: "https://api.electric-sql.cloud",
  credentials: [
    secret: "...",    # required
    source_id: "..."  # optional, required for Electric Cloud
  ]

# application.ex
children = [
  MyApp.Repo,
  # ...
  {MyApp.Endpoint, phoenix_sync: Phoenix.Sync.plug_opts()}
]
```

### Local HTTP services

It is also possible to include Electric as an application dependency and configure it to expose a local HTTP API that's consumed by Phoenix.Sync running in `:http` mode:

```elixir
# mix.exs
defp deps do
  [
    {:electric, ">= 1.0.0-beta.20"},
    {:phoenix_sync, "~> 0.3"}
  ]
end

# config/config.exs
config :phoenix_sync,
  env: config_env(),
  mode: :http,
  http: [
    port: 3000,
  ],
  repo: MyApp.Repo,
  url: "http://localhost:3000"

# application.ex
children = [
  MyApp.Repo,
  # ...
  {MyApp.Endpoint, phoenix_sync: Phoenix.Sync.plug_opts()}
]
```

This is less efficient than running in `:embedded` mode but may be useful for testing or when needing to run an HTTP proxy in front of Electric as part of your development stack.

### Different modes for different envs

Apps using `:http` mode in certain environments can exclude `:electric` as a dependency for that environment. The following example shows how to configure:

- `:embedded` mode in `:dev`
- `:http` mode with a local Electric service in `:test`
- `:http` mode with an external Electric service in `:prod`

With Electric only included and compiled as a dependency in `:dev` and `:test`.

```elixir
# mix.exs
defp deps do
  [
    {:electric, "~> 1.0.0-beta.20", only: [:dev, :test]},
    {:phoenix_sync, "~> 0.3"}
  ]
end

# config/dev.exs
config :phoenix_sync,
  env: config_env(),
  mode: :embedded,
  repo: MyApp.Repo

# config/test.esx
config :phoenix_sync,
  env: config_env(),
  mode: :http,
  http: [
    port: 3000,
  ],
  repo: MyApp.Repo,
  url: "http://localhost:3000"

# config/prod.exs
config :phoenix_sync,
  mode: :http,
  url: "https://api.electric-sql.cloud",
  credentials: [
    secret: "...",    # required
    source_id: "..."  # optional, required for Electric Cloud
  ]

# application.ex
children = [
  MyApp.Repo,
  # ...
  {MyApp.Endpoint, phoenix_sync: Phoenix.Sync.plug_opts()}
]
```

## Notes

### ElectricSQL

ElectricSQL is a [real-time sync engine for Postgres](https://electric-sql.com).

Phoenix.Sync uses Electric to handle the core concerns of partial replication, fan out and data delivery.

### Partial replication

Electric defines partial replication using [Shapes](https://electric-sql.com/docs/guides/shapes).

### Shape definitions

Phoenix.Sync allows shapes to be defined in two ways:

1. using an `Ecto.Query` (or `Ecto.Schema` module)
2. using a keyword list

### Using an `Ecto.Query`

Phoenix.Sync maps Ecto queries to shape definitions. This allows you to control what data syncs where using Ecto.Schema and Ecto.Query.

For example, using a query:

```elixir
sync_render(conn, params, from(t in Todos.Todo, where: t.completed == false))
```

Or using a schema (equivalent to `from(t in Todos.Todo)`):

```elixir
sync_render(conn, params, Todos.Todo)
```

Query support is currently limited to `where` conditions. Support for more complex queries using `join`, `order_by`, `limit` and preloaded association graphs is planned and will be added in Q2 2025.

The static shapes defined using the `sync/2` or `sync/3` router macros do not accept `Ecto.Query` structs as a shape definition. This is to avoid excessive recompilation caused by your router having a compile-time dependency on your `Ecto` schemas.

If you want to add a where-clause filter to a static shape in your router, you can add an explicit [`where` clause](https://electric-sql.com/docs/guides/shapes#where-clause) alongside your `Ecto.Schema` module:

```elixir

sync "/incomplete-todos", Todos.Todo, where: "completed = false"
```

You can also include `replica` (see below) in your static shape definitions:

```elixir

sync "/incomplete-todos", Todos.Todo, where: "completed = false", replica: :full
```

For anything else more dynamic, or to use Ecto queries, you should switch from using the `sync` macros in your router to using `sync_render/3` in a controller.

### Using a keyword list

At minimum a shape requires a `table`. You can think of shapes defined with
just a table name as the sync-equivalent of `SELECT * FROM table`.

The available options are:

- `table` (required). The Postgres table name. Be aware of casing and [Postgres's handling of unquoted upper-case names](https://wiki.postgresql.org/wiki/Don%27t_Do_This#Don.27t_use_upper_case_table_or_column_names).
- `namespace` (optional). The Postgres namespace that the table belongs to. Defaults to `public`.
- `where` (optional). Filter to apply to the synced data in SQL format, e.g. `where: "amount < 1.23 AND colour in ('red', 'green')"`.
- `columns` (optional). The columns to include in the synced data. By default Electric will include all columns in the table. The column list **must** include all primary keys. E.g. `columns: ["id", "title", "amount"]`.
- `replica` (optional). By default Electric will only send primary keys + changed columns on updates. Set `replica: :full` to receive the full row, not just the changed columns.

See the [Electric Shapes guide](https://electric-sql.com/docs/guides/shapes) for more information.
