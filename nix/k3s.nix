# Installs the chart on one-node k3s so a real kubelet publishes the volumes.
# The VM has no internet, so nginx serves the fixture caches and every image
# is preloaded.
{
  lib,
  testers,
  dockerTools,
  writeClosure,
  k3s,
  hello,
  busybox,
  chart,
  csi-image,
  runner-image,
  fixtures,
}:

let
  registrar = dockerTools.pullImage {
    imageName = "registry.k8s.io/sig-storage/csi-node-driver-registrar";
    imageDigest = "sha256:b7fefd08651f00ac4df1a196ed4c621c2451fbb899297fe9fad9e36c667af0c0";
    hash = "sha256-3AJUQwa4Uqf9LyFS9OPnClKYc3lMaASZ/qbjYdk7N6Y=";
    finalImageName = "registry.k8s.io/sig-storage/csi-node-driver-registrar";
    finalImageTag = "v2.18.0";
  };

  # A pod like deploy/hello-pod.yaml that runs `args` with the closure of
  # their program.
  pod = name: args: {
    apiVersion = "v1";
    kind = "Pod";
    metadata.name = name;
    spec = {
      restartPolicy = "Never";
      containers = [
        {
          inherit name args;
          image = "${runner-image.imageName}:${runner-image.imageTag}";
          volumeMounts = [
            {
              name = "nix";
              mountPath = "/nix/store";
            }
          ];
        }
      ];
      volumes = [
        {
          name = "nix";
          csi = {
            driver = "nix-store-csi";
            volumeAttributes.closures = builtins.head args;
          };
        }
      ];
    };
  };
in
testers.runNixOSTest {
  name = "nix-store-csi-k3s";

  nodes.machine = {
    # The airgap images don't fit the default 1 GB.
    virtualisation.diskSize = 4096;
    services.k3s = {
      enable = true;
      # The test doesn't use them.
      disable = [
        "coredns"
        "local-storage"
        "metrics-server"
        "servicelb"
        "traefik"
      ];
      # The airgap images hold k3s's pause image and its Helm installer.
      images = [
        k3s.airgap-images
        registrar
        csi-image
        runner-image
      ];
      autoDeployCharts.nix-store-csi = {
        package = chart;
        targetNamespace = "nix-store-csi-system";
        createNamespace = true;
        values = {
          image = {
            repository = csi-image.imageName;
            tag = csi-image.imageTag;
          };
          # So the plugin reaches nginx on the node.
          hostNetwork = true;
          substituters = [ "http://127.0.0.1/cache-zstd" ];
          trustedPublicKeys = [ (lib.fileContents ./test-1.pub) ];
        };
      };
      # hello exits, and busybox's closure is all but one path of hello's.
      manifests.pods.content = [
        (pod "hello" [ "${hello}/bin/hello" ])
        (pod "sleep" [
          "${busybox}/bin/sleep"
          "inf"
        ])
      ];
    };
    services.nginx = {
      enable = true;
      virtualHosts.localhost.root = fixtures;
    };
  };

  testScript = ''
    plugin = "kubectl -n nix-store-csi-system logs daemonset/nix-store-csi-node -c nix-store-csi"
    volumes = "grep 'kubernetes.io~csi/nix/mount' /proc/mounts"
    views = "ls /var/lib/nix-store-csi/views"

    def phase(pod, want):
        try:
            machine.wait_until_succeeds(
                f"kubectl get pod {pod} -o jsonpath='{{.status.phase}}' | grep -qx {want}",
                timeout=300,
            )
        except Exception:
            print(machine.execute(f"kubectl describe pod {pod}; {plugin} --tail=30")[1])
            raise

    with subtest("both pods run, and share the NARs their closures have in common"):
        phase("hello", "Succeeded")
        t.assertEqual(machine.succeed("kubectl logs hello").strip(), "Hello, world!")
        phase("sleep", "Running")
        t.assertEqual(machine.succeed(f"{plugin} | grep -c ' INFO .*fetching NAR'").strip(), "6")

    with subtest("kubelet unpublishes hello's volume once it's done"):
        machine.wait_until_succeeds(f"[ $({volumes} | wc -l) = 1 ]", timeout=60)
        machine.wait_until_succeeds(f"[ $({views} | wc -l) = 1 ]", timeout=60)

    closure = sorted(machine.succeed("xargs -n1 basename < ${writeClosure [ busybox ]}").split())
    ls = "kubectl exec sleep -- ${busybox}/bin/ls /nix/store"

    with subtest("the pod sees only its closure, on one read-only mount"):
        t.assertEqual(sorted(machine.succeed(ls).split()), closure)
        [mount] = machine.succeed(volumes).splitlines()
        target, options = mount.split()[1], mount.split()[3].split(",")
        for option in ["ro", "nosuid", "nodev"]:
            t.assertIn(option, options)

    with subtest("a restarted kubelet republishes over whatever is at the target"):
        machine.succeed(f"umount {target} && mkdir /tmp/other && mount --bind /tmp/other {target}")
        machine.succeed("systemctl restart k3s")
        machine.wait_until_succeeds(f"{volumes} | grep -q ' ro,'", timeout=300)
        machine.succeed(f"[ $({volumes} | wc -l) = 1 ]")
        t.assertEqual(sorted(machine.succeed(f"ls {target}").split()), closure)

    with subtest("deleting the pod unpublishes its volume"):
        machine.wait_until_succeeds("kubectl delete pod sleep --wait --ignore-not-found", timeout=120)
        machine.wait_until_fails(volumes)
        machine.wait_until_succeeds(f"[ -z \"$({views})\" ]", timeout=60)
  '';
}
