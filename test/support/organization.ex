defmodule Support.Organization do
  use Ecto.Schema

  defmodule Address do
    use Ecto.Schema

    @derive Jason.Encoder

    embedded_schema do
      field :street, :string
      field :number, :integer
    end
  end

  @derive {Jason.Encoder, except: [:__meta__]}

  schema "organizations" do
    field :name, :string
    field :external_id, Support.ULID, prefix: "org"

    embeds_one :address, Address

    timestamps()
  end
end
