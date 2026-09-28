{ inputs, ... }:

{
  # For Grafana outside the cluster, which can't use the chart's ConfigMap
  flake.dashboards.nix-store-csi = ../deploy/helm/nix-store-csi/dashboards/nix-store-csi.json;

  perSystem =
    { pkgs, lib, ... }:
    let
      nix = import ./. {
        inherit pkgs lib;
        inherit (inputs) crane advisory-db;
      };
    in
    {
      inherit (nix) formatter checks;
      packages = nix.packages // {
        default = nix.packages.nix-store-csi;
      };
      devShells = nix.devShells // {
        default = nix.devShells.nix-store-csi;
      };
    };
}
