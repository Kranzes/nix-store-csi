{
  pkgs,
  lib,
  crane,
  advisory-db,
  bombon,
}:

let
  craneLibFor =
    pkgs:
    (crane.mkLib pkgs).overrideScope (
      _: _: { stdenvSelector = p: p.stdenvAdapters.useMoldLinker p.stdenv; }
    );
  craneLib = craneLibFor pkgs;
  bombonLib = import bombon { inherit pkgs; };
  # The images copy their contents in, so the SBOM names them as extraPaths.
  withSbom =
    extraPaths: image:
    image.overrideAttrs (
      finalAttrs: prev: {
        passthru = prev.passthru // {
          sbom = bombonLib.buildBom finalAttrs.finalPackage {
            inherit extraPaths;
            # zstd under pkgsStatic propagates the bash that its scripts use,
            # but the images don't contain it.
            excludes = [ "-bash-static-" ];
          };
        };
      }
    );
  inherit (lib.importTOML ../rust/Cargo.toml) package;
  inherit (packages.nix-store-csi) commonArgs cargoArtifacts;
  chartSrc = ../deploy/helm/nix-store-csi;
  fixtures = pkgs.callPackage ./fixtures.nix { };
  packages = {
    nix-store-csi = pkgs.callPackage ./package.nix { inherit craneLib; };

    # Linked statically against musl, so the plugin's image needs only the
    # binary and CA certificates.
    # The vendored SBOM lists the crates in the SBOM of csi-image.
    nix-store-csi-static =
      let
        package = pkgs.pkgsStatic.callPackage ./package.nix {
          craneLib = craneLibFor pkgs.pkgsStatic;
        };
      in
      bombonLib.passthruVendoredSbom.rust package { pkgs = pkgs.pkgsStatic; };

    csi-image =
      withSbom
        [
          packages.nix-store-csi-static
          pkgs.dockerTools.caCertificates
        ]
        (
          pkgs.dockerTools.buildLayeredImage {
            inherit (package) name;
            tag = package.version;
            # The chart's locality Deployment runs /bin/nix-store-csi.
            contents = [
              pkgs.dockerTools.caCertificates
              packages.nix-store-csi-static
            ];
            config.Entrypoint = [
              (lib.getExe packages.nix-store-csi-static)
              "csi"
            ];
          }
        );

    # Pods run this image with the volume mounted over its empty /nix/store.
    runner-image =
      let
        image = pkgs.callPackage ../deploy/runner-image.nix {
          name = "${package.name}-runner";
          tag = package.version;
        };
      in
      withSbom image.copiedPaths image;

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

  devShells.nix-store-csi = pkgs.mkShell.override { stdenv = craneLib.stdenvSelector pkgs; } {
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
          helm lint --strict ${chartSrc} --set serviceAccountToken.audience=niks3,netrcSecret=netrc,s3CredentialsSecret=s3,locality.enabled=true \
            --set locality.nodeSelector.pool=system --set-json 'locality.tolerations=[{"key":"pool","value":"system","effect":"NoSchedule"}]' \
            --set-json 'serviceAccount.annotations={"eks.amazonaws.com/role-arn":"arn:aws:iam::111122223333:role/nix-store-csi"}' \
            --set-json 'substituters=["https://cache.nixos.org","s3://my-cache?region=eu-west-1","file:///srv/cache"]'
          helm lint --strict ${chartSrc} --set metrics.enabled=true,metrics.podMonitor.enabled=true,metrics.grafanaDashboard.enabled=true,metrics.prometheusRule.enabled=true
          # The alerts' rules, with each kind of alert.
          rendered() {
            helm template nix-store-csi ${chartSrc} --namespace default \
              --set metrics.enabled=true,metrics.podMonitor.enabled=true,metrics.prometheusRule.enabled=true,locality.enabled=true \
              --set 'prefetch.storePaths=/nix/store/00000000000000000000000000000000-hello' \
              --show-only "templates/$1.yaml"
          }
          rendered prometheusrule | yq .spec > rules.yaml
          promtool check rules rules.yaml
          hash=$(printf %s "$(rendered prefetch | yq .data.storePaths)" | sha256sum | head -c 12)
          sed "s/@hash@/$hash/g" ${./alerts-test.yaml} > alerts-test.yaml
          promtool test rules alerts-test.yaml
          helm lint --strict ${chartSrc} --set 'admissionPolicy.allowedNamespaces={default,ci}'
          helm lint --strict ${chartSrc} --set 'prefetch.storePaths\.x86_64-linux=/nix/store/00000000000000000000000000000000-hello'
          helm lint --strict ${chartSrc} --set metrics.enabled=true,metrics.prometheusRule.enabled=true --set-json 'nodePools={"gpu":{"nodeSelector":{"pool":"gpu","accel":"nvidia"},"prefetch":{"storePaths":"/nix/store/00000000000000000000000000000000-hello"}},"web":{"nodeSelector":{"pool":"web"}}}'
          # Each DaemonSet mounts the ConfigMap that its prefetchConfigMap
          # names, which may not exist yet, and pulls with imagePullSecrets.
          pools=(--set prefetchConfigMap=lists --set 'imagePullSecrets={registry}' \
            --set-json 'nodePools={"gpu":{"nodeSelector":{"pool":"gpu"},"prefetchConfigMap":"gpu-lists"}}')
          helm lint --strict ${chartSrc} "''${pools[@]}"
          helm template ${chartSrc} "''${pools[@]}" | yq 'select(.kind == "DaemonSet")' > daemonsets.yaml
          mounts=$(yq -N '.spec.template.spec.volumes[] | select(.name == "prefetch") | .configMap.name + " " + (.configMap.optional | tostring)' daemonsets.yaml)
          test "$mounts" = "$(printf 'lists true\ngpu-lists true')"
          secrets=$(yq -N '.spec.template.spec.imagePullSecrets[].name' daemonsets.yaml)
          test "$secrets" = "$(printf 'registry\nregistry')"
          # A pool's maxStoreSize replaces the top-level one, and an empty one
          # turns it off.
          limits=$(helm template ${chartSrc} --set maxStoreSize=200G \
            --set-json 'nodePools={"gpu":{"nodeSelector":{"pool":"gpu"},"maxStoreSize":"80%"},"web":{"nodeSelector":{"pool":"web"},"maxStoreSize":""}}' \
            | yq -N 'select(.kind == "DaemonSet") | .metadata.name as $name | .spec.template.spec.containers[0].args[] | select(test("^--max-store-size")) | $name + " " + .')
          test "$limits" = "$(printf '%s\n' 'release-name-node --max-store-size=200G' 'release-name-node-gpu --max-store-size=80%')"
          # helm lint ignores fail, so these use helm template.
          helm template ${chartSrc} --set-json 'trustedPublicKeys=[]' --set-json 'extraArgs=["--no-require-sigs"]' > /dev/null
          helm template ${chartSrc} --set-json 'trustedPublicKeys=[]' --set-json 'extraArgs=["--trusted-public-keys=cache-1:key"]' > /dev/null
          # Fails unless helm template refuses the values in the other
          # arguments with the error in the first.
          refuses() {
            if helm template ${chartSrc} "''${@:2}" > /dev/null 2> err; then
              echo "helm template accepted ''${*:2}" >&2
              exit 1
            fi
            grep -F "$1" err
          }
          refuses "trustedPublicKeys is empty" --set-json 'trustedPublicKeys=[]'
          refuses "prefetch and prefetchConfigMap are both set" \
            --set 'prefetch.storePaths=/nix/store/00000000000000000000000000000000-hello,prefetchConfigMap=lists'
          refuses "nodePools.gpu.prefetch and nodePools.gpu.prefetchConfigMap are both set" \
            --set-json 'nodePools={"gpu":{"nodeSelector":{"pool":"gpu"},"prefetch":{"storePaths":"/nix/store/00000000000000000000000000000000-hello"},"prefetchConfigMap":"gpu-lists"}}'
          refuses "at '/maxStoreSize'" --set maxStoreSize=150%
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
      inherit (packages)
        nix-store-csi
        chart
        csi-image
        runner-image
        ;
    };
  };
}
