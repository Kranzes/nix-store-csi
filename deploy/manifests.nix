# Fills in the NIX_STORE_PATH_ variables in the manifests in ./manifests, and
# writes the chart's prefetch value for the same store paths to its prefetch
# output.
# See "Injecting store paths into manifests" in README.md.
{
  lib,
  runCommand,
  gettext,
  hello,
}:
let
  # Each name becomes NIX_STORE_PATH_<name> in the manifests
  storePaths = {
    HELLO = hello;
  };
  storePath = path: builtins.unsafeDiscardStringContext "${path}";
  # A set that mixes systems, as in "Multiple architectures" in README.md, gets
  # a list for each.
  prefetch = lib.mapAttrs' (
    system: paths:
    lib.nameValuePair "storePaths.${system}" (lib.concatMapStringsSep " " storePath paths)
  ) (lib.groupBy (path: path.stdenv.hostPlatform.system) (lib.attrValues storePaths));
in
runCommand "manifests"
  {
    __structuredAttrs = true;
    outputs = [
      "out"
      "prefetch"
    ];
    nativeBuildInputs = [ gettext ];
    env = lib.mapAttrs' (
      name: path: lib.nameValuePair "NIX_STORE_PATH_${name}" (storePath path)
    ) storePaths;
    prefetchValues = builtins.toJSON { inherit prefetch; };
  }
  ''
    format=$(printf '$%s ' "''${!NIX_STORE_PATH_@}")
    cd ${./manifests}
    find . -type f | while read -r file; do
      for name in $(envsubst --variables "$(< "$file")"); do
        if [[ $name == NIX_STORE_PATH_* && ! -v $name ]]; then
          echo "$file uses $name, which storePaths doesn't set" >&2
          exit 1
        fi
      done
      mkdir -p "$out/$(dirname "$file")"
      envsubst "$format" < "$file" > "$out/$file"
    done
    # JSON is YAML, so helm takes it with -f.
    echo "$prefetchValues" > "$prefetch"
  ''
