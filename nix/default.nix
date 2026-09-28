{
  pkgs,
  lib,
  crane,
  advisory-db,
}:

let
  craneLib = crane.mkLib pkgs;
  inherit (lib.importTOML ../rust/Cargo.toml) package;
  inherit (packages.nix-store-csi) commonArgs cargoArtifacts;
  chartSrc = ../deploy/helm/nix-store-csi;
  fixtures = pkgs.callPackage ./fixtures.nix { };
  packages = {
    nix-store-csi = pkgs.callPackage ./package.nix { inherit craneLib; };

    csi-image = pkgs.dockerTools.buildLayeredImage {
      inherit (package) name;
      tag = package.version;
      contents = [ pkgs.dockerTools.caCertificates ];
      config.Entrypoint = [
        (lib.getExe packages.nix-store-csi)
        "csi"
      ];
    };

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

    chart =
      pkgs.runCommand "${package.name}-chart.tgz" { nativeBuildInputs = [ pkgs.kubernetes-helm ]; }
        ''
          HOME=$TMPDIR helm package ${chartSrc} --destination .
          mv *.tgz $out
        '';
  };
in
rec {
  inherit packages;

  devShells.nix-store-csi = pkgs.mkShell {
    inherit (package) name;
    inputsFrom = [ packages.nix-store-csi ];
    env = {
      NIX_STORE_CSI_FIXTURES = fixtures;
      inherit (commonArgs.env) CSI_PROTO;
    };
    packages = [
      formatter
      pkgs.clippy
      pkgs.rust-analyzer
      pkgs.rustfmt
      pkgs.bacon
    ];
  };

  formatter = pkgs.treefmt.withConfig {
    settings.formatter = {
      nixfmt = {
        command = lib.getExe pkgs.nixfmt;
        includes = [ "*.nix" ];
      };

      rustfmt = {
        command = lib.getExe' pkgs.rustfmt "rustfmt";
        options = [
          "--edition"
          package.edition
        ];
        includes = [ "*.rs" ];
      };

      yamlfmt = {
        command = lib.getExe pkgs.yamlfmt;
        options = [
          "-formatter"
          "retain_line_breaks_single=true"
        ];
        includes = [
          "*.yaml"
          "*.yml"
        ];
        # Go templates, not YAML.
        excludes = [ "deploy/helm/nix-store-csi/templates/*" ];
      };
    };
  };

  checks = {
    formatting = formatter.check ../.;

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
          root = ../rust;
          fileset = lib.fileset.unions [
            packages.nix-store-csi.fileset
            ../rust/deny.toml
          ];
        };
      }
    );

    audit = craneLib.cargoAudit {
      inherit (commonArgs) src;
      inherit advisory-db;
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

    nixos-test = pkgs.callPackage ./test.nix {
      inherit fixtures;
      inherit (packages) chart csi-image runner-image;
    };
  };
}
