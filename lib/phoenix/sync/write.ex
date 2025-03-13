defmodule Phoenix.Sync.Write do
  @moduledoc """


  ```elixir
  Phoenix.Sync.Write.new(conn, [
    # just use the model's changeset to validate
    Todos.Todo,
    # use the `validate_write/2` function to map the write to a changeset
    {Todos.Todo, Todos.Todo.validate_write(&1, conn.params)},
    # map a table name to a changeset function
    {{"public", "todos"}, &Todos.Todo.changeset(&1, &2, conn)}
    {Todos.Todo,
      fetch: &Todos.fetch/1,
      insert: &Todos.Todo.insert_changeset(&1, &2, conn),
      update: &Todos.Todo.update_changeset(&1, &2, conn),
      delete: &Todos.Todo.delete_changeset(&1, &2, conn),
      effects: &Todos.Todo.apply_tx/2
    }
  ])
  ```
  actually, the examples show a single table in the data -- this is not a
  merged change log?
  """

  defmodule Relation do
    defstruct [:prefix, :table]

    defimpl Ecto.Queryable do
      def to_query(relation) do
        %Ecto.Query{
          from: %Ecto.Query.FromExpr{source: {relation.table, nil}, prefix: relation.prefix}
        }
      end
    end
  end

  def new(%Plug.Conn{} = conn, config) do
    conn = conn |> Plug.Conn.fetch_query_params()
    new(conn, conn.params, config)
  end

  def new(%Plug.Conn{} = conn, params, config) do
    updates = write_list(conn)

    applicators = build_applicators(config)

    updates
    |> Enum.reduce(Ecto.Multi.new(), &apply_update(&1, &2, changeset_for_event))
  end

  # have two things: 
  # - validate a change, so like a changeset but with authz
  # - for all changes this needs (%module{}, data)
  # - for non-schema based things, how to get the orig struct, or 
  #   for those do we just use {}
  #
  defp build_applicators(config) do
    Enum.map(config, fn
      module when is_atom(module) ->
        Code.ensure_loaded!(module)
        namespace = module.__schema__(:prefix) || "public"
        table = module.__schema__(:source)
        # maybe should be a list to match the relation in the update
        {{namespace, table}, module, &module.changeset/2}

      {{namespace, table}, changeset_fun}
      when is_binary(namespace) and is_binary(table) and is_function(changeset_fun, 2) ->
        {{namespace, table}, %Relation{prefix: namespace, table: table}, changeset_fun}
    end)
  end

  defp changeset_for_event(%{"operation" => "insert"} = change, multi, applicators) do
  end

  defp write_list(conn) do
    # https://hexdocs.pm/plug/Plug.Parsers.JSON.html
    conn.body_params["_json"]
  end
end
