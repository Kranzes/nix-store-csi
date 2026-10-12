# Fills in the NIX_STORE_PATH_ variables in the manifests in ./manifests.
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
in
runCommand "manifests"
  {
    __structuredAttrs = true;
    nativeBuildInputs = [ gettext ];
    env = lib.mapAttrs' (
      name: path:
      lib.nameValuePair "NIX_STORE_PATH_${name}" (builtins.unsafeDiscardStringContext "${path}")
    ) storePaths;
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
  ''
