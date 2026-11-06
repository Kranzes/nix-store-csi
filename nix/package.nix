{
  lib,
  stdenv,
  craneLib,
  fetchurl,
  glibc,
  nukeReferences,
  pkg-config,
  protobuf,
  xz,
  zstd,
  # Link statically against glibc, rather than through pkgsStatic and musl.
  static ? false,
}:

let
  inherit (stdenv.hostPlatform.rust) rustcTarget;
  # Static libraries only, so nothing can link them dynamically.
  cLibraries =
    if static then
      [
        (xz.override { enableStatic = true; })
        (zstd.override { static = true; })
      ]
    else
      [
        xz
        zstd
      ];
  fileset = lib.fileset.unions [
    (lib.fileset.fileFilter (f: f.hasExt "rs") ../rust)
    ../rust/Cargo.toml
    ../rust/Cargo.lock
  ];
  commonArgs = {
    src = lib.fileset.toSource {
      root = ../rust;
      inherit fileset;
    };
    strictDeps = true;
    nativeBuildInputs = [
      pkg-config
      protobuf
    ];
    # Not glibc.static, which the build scripts would link against too.
    buildInputs = cLibraries;
    env =
      lib.optionalAttrs static {
        # With the target named, these reach the binary but not the build
        # scripts and proc macros, which break when linked against glibc.static.
        CARGO_BUILD_TARGET = rustcTarget;
        "CARGO_TARGET_${lib.toUpper (builtins.replaceStrings [ "-" ] [ "_" ] rustcTarget)}_RUSTFLAGS" =
          "-C target-feature=+crt-static -L native=${glibc.static}/lib";
      }
      // {
        ZSTD_SYS_USE_PKG_CONFIG = true;
        CSI_PROTO = fetchurl {
          url = "https://raw.githubusercontent.com/container-storage-interface/spec/v1.13.0/csi.proto";
          hash = "sha256-jFYEy3b+//GcAc+I69Ip5PjIjEGYmLB+SRQxjSXYSvM=";
        };
      };
  };
  cargoArtifacts = craneLib.buildDepsOnly commonArgs;
in
craneLib.buildPackage (
  commonArgs
  // {
    inherit cargoArtifacts;
    # checks.tests runs the tests.
    doCheck = false;
  }
  // lib.optionalAttrs static {
    nativeBuildInputs = commonArgs.nativeBuildInputs ++ [ nukeReferences ];
    # glibc names its own store path and libidn2's, for gconv modules and IDN
    # lookups the image doesn't have, which would pull them into the closure.
    postFixup = "nuke-refs $out/bin/*";
    allowedReferences = [ ];
  }
  // {
    passthru = {
      inherit commonArgs cargoArtifacts fileset;
      # What the static binary links in. They leave no reference in it, and
      # SBOM tools see only libraries from pkgsStatic.
      linkedLibraries = lib.optionals static (map lib.getLib cLibraries ++ [ glibc.static ]);
    };
    meta = {
      mainProgram = "nix-store-csi";
      license = lib.licenses.mit;
    };
  }
)
