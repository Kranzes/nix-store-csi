# The image for pods that run a program from the volume, which the runner
# image is built from. Pass extraCertificates to add a CA, like a company's.
# See "Your own image" in README.md.
{
  lib,
  dockerTools,
  pkgsStatic,
  cacert,
  name ? "base",
  tag ? null,
  extraCertificates ? [ ],
}:

dockerTools.buildLayeredImage {
  inherit name tag;
  extraCommands = ''
    install -m 555 ${pkgsStatic.tini}/bin/tini tini
    mkdir -p nix/store tmp etc/ssl/certs etc/pki/tls/certs var/empty
    chmod 1777 tmp
    # Copied, not linked, since the volume hides the store.
    install -m 444 ${dockerTools.fakeNss}/etc/{passwd,group,nsswitch.conf} etc/
    cat ${cacert}/etc/ssl/certs/ca-bundle.crt ${
      lib.concatMapStringsSep " " (cert: "${cert}") extraCertificates
    } > etc/ssl/certs/ca-certificates.crt
    chmod 444 etc/ssl/certs/ca-certificates.crt
    ln -s ca-certificates.crt etc/ssl/certs/ca-bundle.crt
    ln -s ../../../ssl/certs/ca-certificates.crt etc/pki/tls/certs/ca-bundle.crt
  '';
  config = {
    # -s keeps tini reaping zombies when it isn't PID 1.
    Entrypoint = [
      "/tini"
      "-s"
      "--"
    ];
    # nobody in fakeNss. Kubernetes checks runAsNonRoot only against a numeric
    # user.
    User = "65534:65534";
  };
}
