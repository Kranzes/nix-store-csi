# busybox gives the k3s test a pod that stays up. The test prefetches warm,
# which no pod uses until the cache is down.
# The signing key is in the repo so the caches build reproducibly.
{
  runCommand,
  mkBinaryCache,
  writeClosure,
  writeScript,
  hello,
  busybox,
  nix,
}:

let
  warm = writeScript "warm" ''
    #!${busybox}/bin/sh
    echo warm
  '';
  roots = [
    hello
    busybox
    warm
  ];
  cache =
    compression:
    mkBinaryCache {
      name = "cache-${compression}";
      inherit compression;
      rootPaths = roots;
    };
in
runCommand "nix-store-csi-fixtures" { nativeBuildInputs = [ nix ]; } ''
  export HOME=$TMPDIR NIX_STATE_DIR=$TMPDIR/state NIX_CONF_DIR=$TMPDIR
  nix() { command nix --extra-experimental-features nix-command "$@"; }

  mkdir $out
  cp --no-preserve=mode -r ${cache "none"} $out/cache-none
  cp --no-preserve=mode -r ${cache "zstd"} $out/cache-zstd

  cp ${./test-1.pub} $out/public.key
  for c in $out/cache-*; do
    nix store sign --store "file://$c" --key-file ${./test-1.secret} --all
  done

  echo ${hello} > $out/hello.path
  cp ${writeClosure roots} $out/closure
''
// {
  inherit warm;
}
