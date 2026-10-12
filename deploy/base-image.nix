# A base image for pods that keeps store paths out of the image. It adds
# ./company-ca.pem to nixpkgs' CA certificates. See "Your own image" in
# README.md.
{
  dockerTools,
  pkgsStatic,
  cacert,
}:

dockerTools.buildLayeredImage {
  name = "base";
  extraCommands = ''
    install -m 555 ${pkgsStatic.tini}/bin/tini tini
    mkdir -p nix/store tmp etc/ssl/certs
    chmod 1777 tmp
    install -m 444 ${dockerTools.fakeNss}/etc/{passwd,group,nsswitch.conf} etc/
    cat ${cacert}/etc/ssl/certs/ca-bundle.crt ${./company-ca.pem} > etc/ssl/certs/ca-certificates.crt
  '';
  config = {
    Entrypoint = [
      "/tini"
      "-s"
      "--"
    ];
    User = "65534:65534";
  };
}
