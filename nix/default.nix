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

    # Linked statically against musl, so the plugin's image needs only the
    # binary and CA certificates.
    nix-store-csi-static = pkgs.pkgsStatic.callPackage ./package.nix {
      craneLib = crane.mkLib pkgs.pkgsStatic;
    };

    csi-image = pkgs.dockerTools.buildLayeredImage {
      inherit (package) name;
      tag = package.version;
      contents = [ pkgs.dockerTools.caCertificates ];
      config.Entrypoint = [
        (lib.getExe packages.nix-store-csi-static)
        "csi"
      ];
    };

    # Pods run this image with the volume mounted over its empty /nix/store.
    runner-image = pkgs.callPackage ../deploy/runner-image.nix {
      name = "${package.name}-runner";
      tag = package.version;
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
      inherit (commonArgs.env) CSI_PROTO ZSTD_SYS_USE_PKG_CONFIG;
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
        # The chart templates are Go templates, not YAML.
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

    # helm lint renders the templates with the default values, and then with
    # each optional part set.
    chart =
      pkgs.runCommand "${package.name}-chart-lint"
        {
          nativeBuildInputs = [
            pkgs.kubernetes-helm
            pkgs.prometheus.cli
            pkgs.yq-go
          ];
        }
        ''
          export HOME=$TMPDIR
          helm lint --strict ${chartSrc}
          helm lint --strict ${chartSrc} --set serviceAccountToken.audience=niks3,netrcSecret=netrc
          helm lint --strict ${chartSrc} --set metrics.enabled=true,metrics.podMonitor.enabled=true,metrics.grafanaDashboard.enabled=true,metrics.prometheusRule.enabled=true
          # The alerts' rules, with each kind of alert.
          rendered() {
            helm template nix-store-csi ${chartSrc} --namespace default \
              --set metrics.enabled=true,metrics.podMonitor.enabled=true,metrics.prometheusRule.enabled=true \
              --set 'prefetch.storePaths=/nix/store/00000000000000000000000000000000-hello' \
              --show-only "templates/$1.yaml"
          }
          rendered prometheusrule | yq .spec > rules.yaml
          promtool check rules rules.yaml
          revision=$(rendered prefetch | yq .data.revision)
          sed "s/@revision@/$revision/g" ${./alerts-test.yaml} > alerts-test.yaml
          promtool test rules alerts-test.yaml
          helm lint --strict ${chartSrc} --set 'admissionPolicy.allowedNamespaces={default,ci}'
          helm lint --strict ${chartSrc} --set 'prefetch.storePaths\.x86_64-linux=/nix/store/00000000000000000000000000000000-hello'
          helm lint --strict ${chartSrc} --set metrics.enabled=true,metrics.prometheusRule.enabled=true --set-json 'nodePools={"gpu":{"nodeSelector":{"pool":"gpu","accel":"nvidia"},"prefetch":{"storePaths":"/nix/store/00000000000000000000000000000000-hello"}},"web":{"nodeSelector":{"pool":"web"}}}'
          # helm lint ignores fail, so these use helm template.
          helm template ${chartSrc} --set-json 'trustedPublicKeys=[]' --set-json 'extraArgs=["--no-require-sigs"]' > /dev/null
          helm template ${chartSrc} --set-json 'trustedPublicKeys=[]' --set-json 'extraArgs=["--trusted-public-keys=cache-1:key"]' > /dev/null
          if helm template ${chartSrc} --set-json 'trustedPublicKeys=[]' > /dev/null 2> err; then
            echo "helm template accepted empty trustedPublicKeys" >&2
            exit 1
          fi
          grep "trustedPublicKeys is empty" err
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
