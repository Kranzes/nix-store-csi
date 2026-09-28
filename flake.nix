{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    crane.url = "github:ipetkov/crane";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };
  };

  outputs =
    inputs:
    inputs.flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      imports = [ inputs.treefmt-nix.flakeModule ];

      perSystem =
        {
          pkgs,
          lib,
          config,
          ...
        }:
        let
          srcFiles = lib.fileset.unions [
            (lib.fileset.fileFilter (f: f.hasExt "rs") ./.)
            (lib.fileset.fileFilter (f: f.name == "Cargo.toml") ./.)
            ./Cargo.lock
          ];
          src = lib.fileset.toSource {
            root = ./.;
            fileset = srcFiles;
          };
          inherit (lib.importTOML ./Cargo.toml) package;
          fixtures = pkgs.callPackage ./nix/fixtures.nix { };
          csiProto = pkgs.fetchurl {
            url = "https://raw.githubusercontent.com/container-storage-interface/spec/v1.13.0/csi.proto";
            hash = "sha256-jFYEy3b+//GcAc+I69Ip5PjIjEGYmLB+SRQxjSXYSvM=";
          };
          craneLib = inputs.crane.mkLib pkgs;
          commonArgs = {
            inherit src;
            strictDeps = true;
            nativeBuildInputs = [
              pkgs.pkg-config
              pkgs.protobuf
            ];
            buildInputs = [
              pkgs.xz
              pkgs.zstd
            ];
            env = {
              ZSTD_SYS_USE_PKG_CONFIG = true;
              CSI_PROTO = csiProto;
            };
          };
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
          nix-store-csi = craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts;
              # checks.tests runs them.
              doCheck = false;
              meta.mainProgram = package.name;
            }
          );
          chartSrc = ./deploy/helm/nix-store-csi;
        in
        {
          packages = {
            inherit nix-store-csi;
            default = nix-store-csi;

            csi-image = pkgs.dockerTools.buildLayeredImage {
              inherit (package) name;
              tag = package.version;
              contents = [ pkgs.dockerTools.caCertificates ];
              config.Entrypoint = [
                (lib.getExe nix-store-csi)
                "csi"
              ];
            };

            chart =
              pkgs.runCommand "${package.name}-chart.tgz" { nativeBuildInputs = [ pkgs.kubernetes-helm ]; }
                ''
                  HOME=$TMPDIR helm package ${chartSrc} --destination .
                  mv *.tgz $out
                '';

            # Pods run this image with the volume mounted over its empty /nix/store.
            runner-image = pkgs.dockerTools.buildLayeredImage {
              name = "${package.name}-runner";
              tag = package.version;
              extraCommands = ''
                install -m 555 ${pkgs.pkgsStatic.tini}/bin/tini tini
                mkdir -p nix/store tmp etc var/empty
                chmod 1777 tmp
                # Copied, not linked, since the volume hides the store.
                install -m 444 ${pkgs.dockerTools.fakeNss}/etc/{passwd,group,nsswitch.conf} etc/
              '';
              # -s keeps tini reaping zombies when it isn't PID 1.
              config.Entrypoint = [
                "/tini"
                "-s"
                "--"
              ];
            };
          };

          checks = {
            clippy = craneLib.cargoClippy (
              commonArgs
              // {
                inherit cargoArtifacts;
                cargoClippyExtraArgs = "--all-targets -- --deny warnings";
              }
            );

            tests = craneLib.cargoTest (
              commonArgs
              // {
                inherit cargoArtifacts;
                nativeBuildInputs = commonArgs.nativeBuildInputs ++ [ pkgs.cacert ];
                env = commonArgs.env // {
                  NIX_STORE_CSI_FIXTURES = fixtures;
                };
              }
            );

            deny = craneLib.cargoDeny (
              commonArgs
              // {
                src = lib.fileset.toSource {
                  root = ./.;
                  fileset = lib.fileset.unions [
                    srcFiles
                    ./deny.toml
                  ];
                };
              }
            );

            audit = craneLib.cargoAudit {
              inherit src;
              inherit (inputs) advisory-db;
            };

            # Lint also renders the templates with the default values.
            chart =
              pkgs.runCommand "${package.name}-chart-lint" { nativeBuildInputs = [ pkgs.kubernetes-helm ]; }
                ''
                  HOME=$TMPDIR helm lint --strict ${chartSrc}
                  for field in version appVersion; do
                    grep -qx "$field: ${package.version}" ${chartSrc}/Chart.yaml || {
                      echo "Chart.yaml's $field isn't Cargo.toml's ${package.version}" >&2
                      exit 1
                    }
                  done
                  touch $out
                '';

            k3s = pkgs.callPackage ./nix/k3s.nix {
              inherit fixtures;
              inherit (config.packages) chart csi-image runner-image;
            };
          };

          devShells.default = pkgs.mkShell {
            inherit (package) name;
            inputsFrom = [ nix-store-csi ];
            env = {
              NIX_STORE_CSI_FIXTURES = fixtures;
              CSI_PROTO = csiProto;
            };
            packages = with pkgs; [
              clippy
              rust-analyzer
              rustfmt
              bacon
            ];
          };

          treefmt = {
            projectRootFile = "Cargo.toml";
            programs.rustfmt.enable = true;
            programs.nixfmt.enable = true;
            programs.yamlfmt = {
              enable = true;
              settings.formatter.retain_line_breaks_single = true;
              # Go templates, not YAML.
              excludes = [ "deploy/helm/nix-store-csi/templates/*" ];
            };
          };
        };
    };
}
