{
  lib,
  craneLib,
  fetchurl,
  pkg-config,
  protobuf,
  xz,
  zstd,
}:

let
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
    buildInputs = [
      xz
      zstd
    ];
    env = {
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
    # checks.tests runs them.
    doCheck = false;
    passthru = { inherit commonArgs cargoArtifacts fileset; };
    meta = {
      mainProgram = "nix-store-csi";
      license = lib.licenses.mit;
    };
  }
)
