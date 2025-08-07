# Script for populating the database. You can run it as:
#
#     mix run priv/repo/seeds.exs
#
# Inside the script, you can read and write to any of your
# repositories directly:
#
#     PhoenixSyncExample.Repo.insert!(%PhoenixSyncExample.SomeSchema{})
#
# We recommend using the bang functions (`insert!`, `update!`
# and so on) as they will fail if something goes wrong.

alias PhoenixSyncExample.Cars.Make
alias PhoenixSyncExample.Repo

_ford = Repo.insert!(%Make{id: "52e372d3-cb45-401c-8d2a-6e898e99cea3", name: "Ford"})
