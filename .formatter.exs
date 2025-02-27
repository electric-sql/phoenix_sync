locals_without_parens = [
  # Electric.Phoenix.Router
  sync: 1,
  sync: 2,
  sync: 3
]

[
  locals_without_parens: locals_without_parens,
  export: [locals_without_parens: locals_without_parens],
  import_deps: [:plug, :phoenix, :ecto],
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
]
