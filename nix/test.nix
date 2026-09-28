# Installs the chart on one-node k3s so a real kubelet publishes the volumes.
# The VM has no internet, so nginx serves the fixture caches and every image
# is preloaded.
{
  lib,
  testers,
  dockerTools,
  writeClosure,
  writeText,
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

  livenessprobe = dockerTools.pullImage {
    imageName = "registry.k8s.io/sig-storage/livenessprobe";
    imageDigest = "sha256:19f2cf2f40e1987c7943945deca5f65cd4e4bf5cc0657fc78a5df131861578e8";
    hash = "sha256-2MhW8hOLWdqE6o/hLxbaVlGSMIFi83LEOp/lae+rqVs=";
    finalImageName = "registry.k8s.io/sig-storage/livenessprobe";
    finalImageTag = "v2.20.0";
  };

  # A pod like deploy/hello-pod.yaml that runs `args` with the closure of
  # their program.
  pod =
    {
      name,
      args,
      readOnly ? true,
    }:
    {
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
              inherit readOnly;
              driver = "nix-store-csi";
              volumeAttributes.closures = builtins.head args;
            };
          }
        ];
      };
    };
  sleeper =
    name:
    pod {
      inherit name;
      args = [
        "${busybox}/bin/sleep"
        "inf"
      ];
    };

  # Started with the cache down, and a closure that's on the node already.
  offline = writeText "offline.json" (builtins.toJSON (sleeper "offline"));
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
        livenessprobe
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
        (pod {
          name = "hello";
          args = [ (lib.getExe hello) ];
        })
        (sleeper "sleep")
        (pod {
          name = "writable";
          args = [ (lib.getExe hello) ];
          readOnly = false;
        })
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
        restarts = "{.items[0].status.containerStatuses[*].restartCount}"
        t.assertEqual(machine.succeed(f"kubectl -n nix-store-csi-system get pod -o jsonpath='{restarts}'").split(), ["0"] * 3)

    with subtest("the plugin refuses a volume that isn't read-only"):
        machine.wait_until_succeeds(
            "kubectl get events --field-selector involvedObject.name=writable -o jsonpath='{.items[*].message}'"
            " | grep -q 'needs readOnly: true'",
            timeout=60,
        )

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
        [mount] = machine.succeed("kubectl exec sleep -- ${busybox}/bin/grep ' /nix/store ' /proc/mounts").splitlines()
        t.assertIn("ro", mount.split()[3].split(","))

    with subtest("a restarted kubelet republishes over whatever is at the target"):
        machine.succeed(f"umount {target} && mkdir /tmp/other && mount --bind /tmp/other {target}")
        machine.succeed("systemctl restart k3s")
        machine.wait_until_succeeds(f"{volumes} | grep -q ' ro,'", timeout=300)
        machine.succeed(f"[ $({volumes} | wc -l) = 1 ]")
        t.assertEqual(sorted(machine.succeed(f"ls {target}").split()), closure)

    with subtest("a restarted plugin publishes what's on the node with the cache down"):
        machine.succeed("systemctl stop nginx")
        machine.succeed("kubectl -n nix-store-csi-system delete pod -l app.kubernetes.io/name=nix-store-csi --wait")
        machine.succeed("kubectl apply -f ${offline}")
        phase("offline", "Running")
        t.assertEqual(machine.succeed(f"{plugin} | grep -c 'fetching NAR' || true").strip(), "0")

    with subtest("deleting the pods unpublishes their volumes"):
        machine.wait_until_succeeds("kubectl delete pod sleep offline --wait --ignore-not-found", timeout=120)
        machine.wait_until_fails(volumes)
        machine.wait_until_succeeds(f"[ -z \"$({views})\" ]", timeout=60)
  '';
}
