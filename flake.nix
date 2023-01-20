{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane = {
      url = "github:ipetkov/crane";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, crane, fenix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        toolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain;
          sha256 = "sha256-Zk2rxv6vwKFkTTidgjPm6gDsseVmmljVt201H7zuDkk=";
        };
        craneLib = crane.lib.${system}.overrideToolchain toolchain;
        packageDef = {
          src = ./.;
          pname = "sqllogictest";
        };
        cargoArtifacts = craneLib.buildDepsOnly packageDef;
        sqllogictest = craneLib.buildPackage (packageDef // {
          inherit cargoArtifacts;
        });
      in {
        packages.default = sqllogictest;

        apps.default = flake-utils.lib.mkApp {
          drv = sqllogictest;
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = [ sqllogictest ];
          packages = with pkgs; [ rust-analyzer ];
        };
      });
}
