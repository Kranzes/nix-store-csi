# Installs the chart on a one-node k3s cluster so a real kubelet publishes the
# volumes.
# The VM has no internet, so nginx serves the fixture caches and every image
# is preloaded.
{
  lib,
  testers,
  dockerTools,
  writeClosure,
  writeText,
  k3s,
  curl,
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

  # A pod like deploy/hello-pod.yaml that runs `args`, with the closure of its
  # program mounted.
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
        securityContext = {
          runAsNonRoot = true;
          seccompProfile.type = "RuntimeDefault";
        };
        containers = [
          {
            inherit name args;
            image = "${runner-image.imageName}:${runner-image.imageTag}";
            securityContext = {
              allowPrivilegeEscalation = false;
              capabilities.drop = [ "ALL" ];
            };
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
              volumeAttributes.storePaths = builtins.head args;
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

  # The test starts it with the cache down, for a closure already on the node.
  offline = writeText "offline.json" (builtins.toJSON (sleeper "offline"));
in
testers.runNixOSTest {
  name = "nix-store-csi-k3s";

  nodes.machine = {
    # The airgap images don't fit the default 1 GB.
    virtualisation.diskSize = 4096;
    services.k3s = {
      enable = true;
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
        targetNamespace = "kube-system";
        values = {
          image = {
            repository = csi-image.imageName;
            tag = csi-image.imageTag;
          };
          # Puts the plugin on the node's network, where nginx listens on
          # 127.0.0.1.
          hostNetwork = true;
          substituters = [ "http://127.0.0.1/cache-zstd" ];
          trustedPublicKeys = [ (lib.fileContents ./test-1.pub) ];
          netrcSecret = "nix-store-csi-netrc";
          metrics.enabled = true;
        };
      };
      # nginx asks for these credentials.
      manifests.netrc.content = {
        apiVersion = "v1";
        kind = "Secret";
        metadata = {
          name = "nix-store-csi-netrc";
          namespace = "kube-system";
        };
        stringData.netrc = "machine 127.0.0.1 login nix password secret";
      };
      # deploy/README.md says the volumes pass the restricted Pod Security
      # level. Enforcing it on default checks that.
      manifests.pod-security.content = {
        apiVersion = "v1";
        kind = "Namespace";
        metadata = {
          name = "default";
          labels."pod-security.kubernetes.io/enforce" = "restricted";
        };
      };
      # hello exits. hello and busybox share all but one path of their closures.
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
      virtualHosts.localhost = {
        root = fixtures;
        basicAuth.nix = "secret";
      };
    };
  };

  testScript = ''
    plugin = "kubectl -n kube-system logs daemonset/nix-store-csi-node -c nix-store-csi"
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

    with subtest("k3s installs the chart, and the plugin registers its driver"):
        try:
            machine.wait_until_succeeds(
                "kubectl get csinode -o jsonpath='{.items[*].spec.drivers[*].name}' | grep -qw nix-store-csi",
                timeout=300,
            )
        except Exception:
            job = "-n kube-system job/helm-install-nix-store-csi"
            print(machine.execute(f"kubectl describe {job}; kubectl logs {job}")[1])
            raise

    with subtest("both pods run, and the plugin fetches each NAR once"):
        phase("hello", "Succeeded")
        t.assertEqual(machine.succeed("kubectl logs hello").strip(), "Hello, world!")
        phase("sleep", "Running")
        fetched = machine.succeed(f"{plugin} | grep -c ' INFO .*fetching NAR'").strip()
        t.assertEqual(fetched, machine.succeed("wc -l < ${fixtures}/closure").strip())
        restarts = "{.items[0].status.containerStatuses[*].restartCount}"
        t.assertEqual(machine.succeed(f"kubectl -n kube-system get pod -l app.kubernetes.io/name=nix-store-csi -o jsonpath='{restarts}'").split(), ["0"] * 3)

    with subtest("the plugin refuses a volume that isn't read-only"):
        machine.wait_until_succeeds(
            "kubectl get events --field-selector involvedObject.name=writable -o jsonpath='{.items[*].message}'"
            " | grep -q 'needs readOnly: true'",
            timeout=60,
        )

    with subtest("the plugin counts the publishes and fetches in its metrics"):
        metrics = machine.succeed("${curl}/bin/curl -sf http://127.0.0.1:9809/metrics").splitlines()
        machine.succeed("${curl}/bin/curl -sf 'http://[::1]:9809/metrics'")
        def value(sample):
            return sum(float(line.split()[-1]) for line in metrics if line.startswith(sample))
        t.assertGreater(value('nix_store_csi_publish_duration_seconds_count{grpc_code="OK"}'), 0)
        t.assertGreater(value('nix_store_csi_publish_duration_seconds_count{grpc_code="InvalidArgument"}'), 0)
        fetched = [line for line in metrics if line.startswith("nix_store_csi_nar_fetch_duration_seconds_count")]
        t.assertEqual(
            sum(float(line.split()[-1]) for line in fetched if 'result="ok"' in line),
            float(machine.succeed("wc -l < ${fixtures}/closure")),
        )

    with subtest("kubelet unpublishes hello's volume once hello exits"):
        machine.wait_until_succeeds(f"[ $({volumes} | wc -l) = 1 ]", timeout=60)
        machine.wait_until_succeeds(f"[ $({views} | wc -l) = 1 ]", timeout=60)

    closure = sorted(machine.succeed("xargs -n1 basename < ${writeClosure [ busybox ]}").split())
    ls = "kubectl exec sleep -- ${busybox}/bin/ls /nix/store"

    with subtest("the pod sees only its closure, on one read-only mount"):
        t.assertEqual(sorted(machine.succeed(ls).split()), closure)
        t.assertEqual(machine.succeed("kubectl exec sleep -- ${busybox}/bin/stat -c %a /nix/store").strip(), "755")
        [mount] = machine.succeed(volumes).splitlines()
        target, options = mount.split()[1], mount.split()[3].split(",")
        for option in ["ro", "nosuid", "nodev"]:
            t.assertIn(option, options)
        [mount] = machine.succeed("kubectl exec sleep -- ${busybox}/bin/grep ' /nix/store ' /proc/mounts").splitlines()
        t.assertIn("ro", mount.split()[3].split(","))

    with subtest("a restarted kubelet republishes over another mount at the target"):
        machine.succeed(f"umount {target} && mkdir /tmp/other && mount --bind /tmp/other {target}")
        machine.succeed("systemctl restart k3s")
        machine.wait_until_succeeds(f"{volumes} | grep -q ' ro,'", timeout=300)
        machine.succeed(f"[ $({volumes} | wc -l) = 1 ]")
        t.assertEqual(sorted(machine.succeed(f"ls {target}").split()), closure)

    with subtest("a restarted plugin publishes a closure already on the node while the cache is down"):
        machine.succeed("systemctl stop nginx")
        machine.succeed("kubectl -n kube-system delete pod -l app.kubernetes.io/name=nix-store-csi --wait")
        machine.succeed("kubectl apply -f ${offline}")
        phase("offline", "Running")
        t.assertEqual(machine.succeed(f"{plugin} | grep -c 'fetching NAR' || true").strip(), "0")

    with subtest("deleting the pods unpublishes their volumes"):
        machine.wait_until_succeeds("kubectl delete pod sleep offline --wait --ignore-not-found", timeout=120)
        machine.wait_until_fails(volumes)
        machine.wait_until_succeeds(f"[ -z \"$({views})\" ]", timeout=60)
  '';
}
