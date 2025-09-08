defmodule Phoenix.Sync.LiveViewTest do
  use ExUnit.Case, async: true

  alias Phoenix.Sync.LiveViewTest.Endpoint
  alias Electric.Client

  import Phoenix.ConnTest
  import Phoenix.LiveViewTest
  import Phoenix.Component

  require Ecto.Query

  @endpoint Endpoint

  Code.ensure_loaded(Support.User)

  @moduletag :capture_log

  setup do
    {:ok, conn: Plug.Test.init_test_session(build_conn(), %{})}
  end

  import Plug.Conn

  describe "sync_stream/3" do
    test "simple live view with only a snapshot", %{conn: conn} do
      {:ok, client} = Client.Mock.new()

      users = [
        %{id: "6dfea52e-1096-4b62-aafd-838ddd49477d", name: "User 1"},
        %{id: "9fc8f0a7-42e9-4473-9981-43a1904cd88a", name: "User 2"},
        %{id: "4252d858-8764-4069-bb8c-e670f899b80a", name: "User 3"}
      ]

      Client.Mock.async_response(client,
        status: 200,
        schema: %{id: %{type: "uuid"}, name: %{type: "text"}},
        last_offset: Client.Offset.first(),
        shape_handle: "users-1",
        body: Client.Mock.transaction(users, operation: :insert)
      )

      conn =
        conn
        |> put_private(:electric_client, client)
        |> put_private(:test_pid, self())

      {:ok, lv, html} = live(conn, "/stream")

      for %{name: name} <- users do
        assert html =~ name
      end

      users2 = [
        %{id: "53183977-bd54-4171-9697-51e13b0ff7ca", name: "User 4"},
        %{id: "92d42f40-cf16-4d51-a663-171d9fa1a21a", name: "User 5"},
        %{id: "04e15019-010e-4aa1-8eb0-33132099d05b", name: "User 6"}
      ]

      {:ok, _req} =
        Client.Mock.response(client,
          status: 200,
          last_offset: Client.Offset.new(3, 2),
          shape_handle: "users-1",
          body: Client.Mock.transaction(users2, lsn: 3, operation: :insert)
        )

      for _ <- users2 do
        assert_receive {:sync, _}
      end

      html = render(lv)

      for %{name: name} <- users ++ users2 do
        assert html =~ name
      end

      users3 = [
        %{id: "9fc8f0a7-42e9-4473-9981-43a1904cd88a", name: "User 2"},
        %{id: "53183977-bd54-4171-9697-51e13b0ff7ca", name: "User 4"},
        %{id: "04e15019-010e-4aa1-8eb0-33132099d05b", name: "User 6"}
      ]

      {:ok, _req} =
        Client.Mock.response(client,
          status: 200,
          last_offset: Client.Offset.new(4, 2),
          shape_handle: "users-1",
          body: Client.Mock.transaction(users3, lsn: 4, operation: :delete)
        )

      for _ <- users3 do
        assert_receive {:sync, _}
      end

      html = render(lv)

      for %{name: name} <- users3 do
        refute html =~ name
      end
    end

    test "simple live view with snapshot and updates", %{conn: conn} do
      {:ok, client} = Client.Mock.new()

      snapshot_users = [
        %{id: "6dfea52e-1096-4b62-aafd-838ddd49477d", name: "User 1"},
        %{id: "9fc8f0a7-42e9-4473-9981-43a1904cd88a", name: "User 2"},
        %{id: "4252d858-8764-4069-bb8c-e670f899b80a", name: "User 3"}
      ]

      update_users =
        snapshot_users
        |> Enum.map(&Map.update!(&1, :name, fn _name -> "Updated #{&1.id}" end))

      user_updates = Client.Mock.transaction(update_users, lsn: 1234, operation: :update)

      body =
        Client.Mock.transaction(snapshot_users, operation: :insert, up_to_date: false) ++
          user_updates

      Client.Mock.async_response(client,
        status: 200,
        schema: %{id: %{type: "uuid"}, name: %{type: "text"}},
        last_offset: Client.Offset.first(),
        shape_handle: "users-1",
        body: body
      )

      conn =
        conn
        |> put_private(:electric_client, client)
        |> put_private(:test_pid, self())

      {:ok, lv, html} = live(conn, "/stream")

      for %{name: name} <- snapshot_users do
        assert html =~ name
      end

      assert_receive {:sync, _}

      html = render(lv)

      for %{name: name} <- update_users do
        assert html =~ name
      end
    end

    test "view with component", %{conn: conn} do
      {:ok, client} = Client.Mock.new()

      users = [
        %{id: "6dfea52e-1096-4b62-aafd-838ddd49477d", name: "User 1"},
        %{id: "9fc8f0a7-42e9-4473-9981-43a1904cd88a", name: "User 2"},
        %{id: "4252d858-8764-4069-bb8c-e670f899b80a", name: "User 3"}
      ]

      Client.Mock.async_response(client,
        status: 200,
        schema: %{id: %{type: "uuid"}, name: %{type: "text"}},
        last_offset: Client.Offset.first(),
        shape_handle: "users-1",
        body: Client.Mock.transaction(users, operation: :insert)
      )

      conn =
        conn
        |> put_private(:electric_client, client)
        |> put_private(:test_pid, self())

      {:ok, lv, html} = live(conn, "/stream/with-component")

      for %{name: name} <- users do
        assert html =~ name
      end

      users2 = [
        %{id: "53183977-bd54-4171-9697-51e13b0ff7ca", name: "User 4"},
        %{id: "92d42f40-cf16-4d51-a663-171d9fa1a21a", name: "User 5"},
        %{id: "04e15019-010e-4aa1-8eb0-33132099d05b", name: "User 6"}
      ]

      {:ok, _req} =
        Client.Mock.response(client,
          status: 200,
          last_offset: Client.Offset.new(3, 2),
          shape_handle: "users-1",
          body: Client.Mock.transaction(users2, lsn: 3, operation: :insert)
        )

      for _ <- users2 do
        assert_receive {:sync, _}
      end

      # assert that the component received the :live status event
      assert_receive {:sync, :users, :live}

      html = render(lv)

      for %{name: name} <- users ++ users2 do
        assert html =~ name
      end
    end

    test "errors from the client are returned", %{conn: conn} do
      {:ok, client} = Client.Mock.new()

      Client.Mock.async_response(client,
        status: 400,
        body: [%{"message" => "this is wrong"}]
      )

      conn =
        conn
        |> put_private(:electric_client, client)
        |> put_private(:test_pid, self())

      try do
        live(conn, "/stream")
        flunk("the stream should have aborted")
      catch
        :exit, {{error, _stacktrace}, _} ->
          assert %Electric.Client.Error{message: %{"message" => "this is wrong"}} = error
      end
    end

    defp add_message_keys(txn) do
      Enum.map(txn, fn
        %{"value" => %{"id" => id}} = msg ->
          Map.put(msg, "key", ~s|"public"."users"/"#{id}"|)

        msg ->
          msg
      end)
    end

    test "allows for keyword-based shapes", %{conn: conn} do
      {:ok, client} = Client.Mock.new()

      users = [
        %{id: "6dfea52e-1096-4b62-aafd-838ddd49477d", name: "User 1"},
        %{id: "9fc8f0a7-42e9-4473-9981-43a1904cd88a", name: "User 2"},
        %{id: "4252d858-8764-4069-bb8c-e670f899b80a", name: "User 3"}
      ]

      Client.Mock.async_response(client,
        status: 200,
        schema: %{id: %{type: "uuid", pk_position: 0}, name: %{type: "text"}},
        last_offset: Client.Offset.first(),
        shape_handle: "users-1",
        # TODO: update the mock client to add the key by default
        body: Client.Mock.transaction(users, operation: :insert) |> add_message_keys()
      )

      conn =
        conn
        |> put_private(:electric_client, client)
        |> put_private(:test_pid, self())

      {:ok, lv, html} = live(conn, "/stream/keyword")

      for %{name: name} <- users do
        assert html =~ name
      end

      users2 = [
        %{id: "53183977-bd54-4171-9697-51e13b0ff7ca", name: "User 4"},
        %{id: "92d42f40-cf16-4d51-a663-171d9fa1a21a", name: "User 5"},
        %{id: "04e15019-010e-4aa1-8eb0-33132099d05b", name: "User 6"}
      ]

      {:ok, _req} =
        Client.Mock.response(client,
          status: 200,
          last_offset: Client.Offset.new(3, 2),
          shape_handle: "users-1",
          body: Client.Mock.transaction(users2, lsn: 3, operation: :insert) |> add_message_keys()
        )

      for _ <- users2 do
        assert_receive {:sync, _}
      end

      html = render(lv)

      for %{name: name} <- users ++ users2 do
        assert html =~ name
      end
    end
  end

  describe "electric_client_configuration/1" do
    def client!(opts \\ []) do
      Electric.Client.new!(
        base_url: "https://cloud.electric-sql.com",
        authenticator:
          Keyword.get(
            opts,
            :authenticator,
            {Electric.Client.Authenticator.MockAuthenticator, salt: "my-salt"}
          )
      )
    end

    test "generates a script tag with the right configuration" do
      shape = Ecto.Query.where(Support.User, visible: true)

      html =
        Phoenix.LiveViewTest.render_component(
          &Phoenix.Sync.LiveView.electric_client_configuration/1,
          shape: shape,
          client: client!(),
          key: "visible_user_config"
        )

      assert html =~ ~r/window\.visible_user_config = \{/
      assert html =~ ~r["url":"https://cloud.electric-sql.com/v1/shape"]
      assert html =~ ~r|"electric-mock-auth":"[a-z0-9]+"|
      assert html =~ ~r|"table":"users"|
      assert html =~ ~r|"where":"\(\\"visible\\" = TRUE\)"|
    end

    test "allows for overriding how the configuration is used" do
      assigns = %{}

      html =
        Phoenix.LiveViewTest.rendered_to_string(~H"""
          <div>
            <Phoenix.Sync.LiveView.electric_client_configuration client={client!()} shape={Ecto.Query.where(Support.User, visible: true)}>
              <:script :let={configuration} phx-no-curly-interpolation>
                root.render(React.createElement(MyApp, { client_config: <%= configuration %> }, null))
              </:script>
            </Phoenix.Sync.LiveView.electric_client_configuration>
          </div>
        """)

      assert html =~ ~r/React\.createElement.+client_config: \{/
      assert html =~ ~r["url":"https://cloud.electric-sql.com/v1/shape"]
      assert html =~ ~r|"electric-mock-auth":"[a-z0-9]+"|
      assert html =~ ~r|"table":"users"|
      assert html =~ ~r|"where":"\(\\"visible\\" = TRUE\)"|
    end
  end
end
