# The runner image, for pods that run a program from the volume. Override
# extraCertificates to add a CA, like a company's. See "Your own image" in
# README.md.
{
  lib,
  dockerTools,
  pkgsStatic,
  cacert,
  name ? "runner",
  tag ? null,
  extraCertificates ? [ ],
}:

dockerTools.buildLayeredImage {
  inherit name tag;
  extraCommands = ''
    install -m 555 ${pkgsStatic.tini}/bin/tini tini
    mkdir -p nix/store tmp etc/ssl/certs etc/pki/tls/certs var/empty home/nobody
    chmod 1777 tmp
    # Copied, not linked, since the volume hides the store.
    install -m 444 ${dockerTools.fakeNss}/etc/{group,nsswitch.conf} etc/
    sed 's|^\(nobody:.*\):/var/empty:|\1:/home/nobody:|' ${dockerTools.fakeNss}/etc/passwd > etc/passwd
    chmod 444 etc/passwd
    cat ${cacert}/etc/ssl/certs/ca-bundle.crt ${
      lib.concatMapStringsSep " " (cert: "${cert}") extraCertificates
    } > etc/ssl/certs/ca-certificates.crt
    chmod 444 etc/ssl/certs/ca-certificates.crt
    ln -s ca-certificates.crt etc/ssl/certs/ca-bundle.crt
    ln -s ../../../ssl/certs/ca-certificates.crt etc/pki/tls/certs/ca-bundle.crt
  '';
  fakeRootCommands = ''
    chown 65534:65534 home/nobody
  '';
  config = {
    # nixpkgs builds OpenSSL to look in its own store path, which the volume
    # hides, and patches it, GnuTLS, curl and certifi to read NIX_SSL_CERT_FILE.
    Env = [
      "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt"
      "NIX_SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt"
    ];
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
