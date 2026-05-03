{
  description = "A pinned version of Jupyter for data development.";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }: flake-utils.lib.eachDefaultSystem (system:
    let
      pkgs = import nixpkgs { inherit system; };
    in
    {
      apps.default = {
        type = "app";
        program = "${pkgs.jupyter}/bin/jupyter-notebook";
      };
      formatter = pkgs.nixpkgs-fmt;
    }
  );
}
