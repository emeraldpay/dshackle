{ system ? builtins.currentSystem
, nixpkgs ? import ./nixpkgs.nix { inherit system; }
}:
let
  mkEnv = nixpkgs.callPackage ./mkEnv.nix { };
in
rec {
  inherit
    nixpkgs
    ;

  protoc-gen-grpc-java = nixpkgs.callPackage ./pkgs/protoc-gen-grpc-java { };

  env = mkEnv {
    env = {
      # Configure nix to use nixpgks
      NIX_PATH = "nixpkgs=${toString nixpkgs.path}";
      # Set the env for gradle
      JAVA_HOME = "@env-root@/lib/openjdk";
    };

    paths = [
      # Development tools
      nixpkgs.gitAndTools.git-absorb
      nixpkgs.jq
      nixpkgs.niv
      nixpkgs.websocat
      nixpkgs.yq

      # Protobuf stuff
      nixpkgs.protobuf3_9
      protoc-gen-grpc-java

      # Code formatting
      nixpkgs.treefmt
      nixpkgs.nixpkgs-fmt
      nixpkgs.shfmt

      nixpkgs.jdk
      nixpkgs.coreutils
      nixpkgs.gnugrep
      nixpkgs.which
    ];
  };
}
