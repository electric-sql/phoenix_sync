<p>
  <br />
  <a href="https://hexdocs.pm/phoenix_sync" target="_blank">
    <picture>
      <img alt="Phoenix sync illustration"
          src="./docs/phoenix-sync.png"
      />
    </picture>
  </a>
  <br />
</p>

# Phoenix.Sync

[![Hex.pm](https://img.shields.io/hexpm/v/phoenix_sync.svg)](https://hex.pm/packages/phoenix_sync)
[![Documentation](https://img.shields.io/badge/docs-hexdocs-green)](https://hexdocs.pm/phoenix_sync)
[![License](https://img.shields.io/badge/license-Apache_2.0-green)](./LICENSE)
[![Status](https://img.shields.io/badge/status-beta-orange)](https://github.com/electric-sql/phoenix_sync)
[![Discord](https://img.shields.io/discord/933657521581858818?color=5969EA&label=discord)](https://discord.electric-sql.com)

Sync is the best way of building modern apps. Phoenix.Sync enables real-time sync for Postgres-backed [Phoenix](https://www.phoenixframework.org/) applications.

Documentation is available at [hexdocs.pm/phoenix_sync](https://hexdocs.pm/phoenix_sync).

## Build real-time apps on locally synced data

- sync data into Elixir, `LiveView` and frontend web and mobile applications
- integrates with `Plug` and `Phoenix.{Controller, LiveView, Router, Stream}`
- uses [ElectricSQL](https://electric-sql.com) for scalable data delivery and fan out
- maps `Ecto` queries to [Shapes](https://electric-sql.com/docs/guides/shapes) for partial replication

## Usage

There are four key APIs:

- [`Phoenix.Sync.Client.stream/2`](https://hexdocs.pm/phoenix_sync/Phoenix.Sync.Client.html#stream/2) for low level usage in Elixir
- [`Phoenix.Sync.LiveView.sync_stream/4`](https://hexdocs.pm/phoenix_sync/Phoenix.Sync.LiveView.html#sync_stream/4) to sync into a LiveView stream
- [`Phoenix.Sync.Router.sync/2`](https://hexdocs.pm/phoenix_sync/Phoenix.Sync.Router.html#sync/2) macro to expose a statically defined shape in your Router
- [`Phoenix.Sync.Controller.sync_render/3`](https://hexdocs.pm/phoenix_sync/Phoenix.Sync.Controller.html#sync_render/3) to expose dynamically constructed shapes from a Controller

### Low level usage in Elixir

Use [`Phoenix.Sync.Client.stream/2`](https://hexdocs.pm/phoenix_sync/Phoenix.Sync.Client.html#stream/2) to convert an `Ecto.Query` into an Elixir `Stream`:

```elixir
stream = Phoenix.Sync.Client.stream(Todos.Todo)

stream =
  Ecto.Query.from(t in Todos.Todo, where: t.completed == false)
  |> Phoenix.Sync.Client.stream()
```

### Sync into a LiveView stream

Swap out `Phoenix.LiveView.stream/3` for [`Phoenix.Sync.LiveView.sync_stream/4`](https://hexdocs.pm/phoenix_sync/Phoenix.Sync.LiveView.html#sync_stream/4) to automatically keep a LiveView up-to-date with the state of your Postgres database:

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

LiveView takes care of automatically keeping the front-end up-to-date with the assigned stream. What Phoenix.Sync does is automatically keep the *stream* up-to-date with the state of the database.

This means you can build fully end-to-end real-time multi-user applications without writing Javascript *and* without worrying about message delivery, reconnections, cache invalidation or polling the database for changes.

### Sync shapes through your Router

Use the [`Phoenix.Sync.Router.sync/2`](https://hexdocs.pm/phoenix_sync/Phoenix.Sync.Router.html#sync/2) macro to expose statically (compile-time) defined shapes in your Router:

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

Because the shapes are exposed through your Router, the client connects through your existing Plug middleware. This allows you to do real-time sync straight out of Postgres *without* having to translate your auth logic into complex/fragile database rules.

### Sync dynamic shapes from a Controller

Sync shapes from any standard Controller using the [`Phoenix.Sync.Controller.sync_render/3`](https://hexdocs.pm/phoenix_sync/Phoenix.Sync.Controller.html#sync_render/3) view function:

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

You can sync *into* any client in any language that [speaks HTTP and JSON](https://electric-sql.com/docs/api/http).

For example, using the Electric [Typescript client](https://electric-sql.com/docs/api/clients/typescript):

```typescript
import { Shape, ShapeStream } from '@electric-sql/client'

const stream = new ShapeStream({
  url: `/shapes/todos`
})
const shape = new Shape(stream)

// The callback runs every time the data changes.
shape.subscribe(data => console.log(data))
```

Or binding a shape to a component using the [React bindings](https://electric-sql.com/docs/integrations/react):

```tsx
import { useShape } from '@electric-sql/react'

const MyComponent = () => {
  const { data } = useShape({
    url: `shapes/todos`
  })

  return (
    <List todos={data} />
  )
}
```

See the Electric [demos](https://electric-sql.com/demos) and [documentation](https://electric-sql.com/demos) for more client-side usage examples.

## Installation and configuration

`Phoenix.Sync` can be used in two modes:

1. `:embedded` where Electric is included as an application dependency and Phoenix.Sync consumes data internally using Elixir APIs
2. `:http` where Electric does *not* need to be included as an application dependency and Phoenix.Sync consumes data from an external Electric service using it's [HTTP API](https://electric-sql.com/docs/api/http)

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
  mode: :http,
  http: [
    # https://hexdocs.pm/bandit/Bandit.html#t:options/0
    ip: :loopback,
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
  mode: :embedded,
  repo: MyApp.Repo

# config/test.esx
config :phoenix_sync,
  mode: :http,
  http: [
    ip: :loopback,
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

Phoenix.Sync maps Ecto queries to shape definitions. This allows you to control what data syncs where using Ecto.Schema and Ecto.Query.