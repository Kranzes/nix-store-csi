# Installs the chart on a one-node k3s cluster so a real kubelet publishes the
# volumes.
# The VM has no internet, so nginx serves the fixture caches, versitygw serves
# them as S3 buckets, and every image is preloaded.
{
  lib,
  stdenv,
  testers,
  dockerTools,
  writeClosure,
  writeText,
  runCommand,
  k3s,
  skopeo,
  zstd,
  curl,
  jq,
  versitygw,
  hello,
  busybox,
  nix-store-csi,
  chart,
  csi-image,
  runner-image,
  fixtures,
}:

let
  # One image from k3s's airgap images. Kubelet starts only once k3s imports
  # the pause image, and the whole tarball takes longer.
  airgapImage =
    name:
    runCommand "k3s-airgap-${name}.tar"
      {
        nativeBuildInputs = [
          jq
          skopeo
          zstd
        ];
      }
      ''
        zstd -d ${k3s.airgap-images} -o images.tar
        tar -xf images.tar manifest.json
        tag=$(jq -r '.[].RepoTags[] | select(contains("/${name}:"))' manifest.json)
        skopeo --insecure-policy copy docker-archive:images.tar:"$tag" docker-archive:$out:docker.io/"$tag"
      '';

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
      volumeAttributes ? {
        storePaths = builtins.head args;
      },
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
              inherit readOnly volumeAttributes;
              driver = "nix-store-csi";
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

  other = writeText "other.json" (builtins.toJSON (sleeper "other"));
  # A pod without the driver's volumes. Without a service account token it
  # has no volumes at all.
  plain =
    automountServiceAccountToken:
    writeText "plain.json" (
      builtins.toJSON {
        apiVersion = "v1";
        kind = "Pod";
        metadata.name = "plain";
        spec = {
          inherit automountServiceAccountToken;
          containers = [
            {
              name = "plain";
              image = "${runner-image.imageName}:${runner-image.imageTag}";
            }
          ];
        };
      }
    );

  # What the plugin fetches for the pods. The fixtures also hold warm.
  podsClosure = writeClosure [
    hello
    busybox
  ];

  # The test starts it with the cache down, for a closure that only the
  # plugin's prefetch fetched.
  warm = writeText "warm.json" (
    builtins.toJSON (pod {
      name = "warm";
      args = [ "${fixtures.warm}" ];
    })
  );
  prefetchWarm = writeText "prefetch.json" (
    builtins.toJSON {
      data = {
        "storePaths.${stdenv.hostPlatform.system}" = "${fixtures.warm}";
        revision = "warm";
      };
    }
  );
in
testers.runNixOSTest {
  name = "nix-store-csi-k3s";

  nodes.machine = {
    # The images don't fit the default 1 GB.
    virtualisation.diskSize = 4096;
    virtualisation.cores = 4;
    virtualisation.memorySize = 2048;
    services.k3s = {
      enable = true;
      # Kubelet updates the mounted ConfigMaps when it syncs a pod, and the
      # volume stats every period, both a minute by default.
      extraFlags = [
        "--kubelet-arg=sync-frequency=5s"
        "--kubelet-arg=volume-stats-agg-period=5s"
      ];
      disable = [
        "coredns"
        "local-storage"
        "metrics-server"
        "servicelb"
        "traefik"
      ];
      images = [
        (airgapImage "mirrored-pause")
        (airgapImage "klipper-helm")
        registrar
        livenessprobe
        csi-image
        runner-image
      ];
      autoDeployCharts.nix-store-csi = {
        package = chart;
        targetNamespace = "kube-system";
        # Runs the Helm job on the node's network, so it doesn't wait for
        # flannel.
        extraFieldDefinitions.spec.bootstrap = true;
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
          admissionPolicy.allowedNamespaces = [ "default" ];
          # The other store path doesn't exist, so the prefetch succeeds only
          # if the plugin picks this system's.
          prefetch = {
            "storePaths.${stdenv.hostPlatform.system}" = "${busybox}";
            storePaths = "/nix/store/00000000000000000000000000000000-missing";
          };
          # The test moves the node into it at the end.
          nodePools.warm = {
            nodeSelector.pool = "warm";
            prefetch."storePaths.${stdenv.hostPlatform.system}" = "${fixtures.warm}";
          };
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
      # docs/DEPLOY.md says the volumes pass the restricted Pod Security
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
        # One manifest for multiple systems. The other store paths don't
        # exist, so the pod runs only if the plugin picks this system's.
        (pod {
          name = "hello";
          args = [ "/nix/store/.roots/${hello.name}/bin/hello" ];
          volumeAttributes = {
            "storePaths.${stdenv.hostPlatform.system}" = "${hello}";
            "storePaths.riscv64-linux" = "/nix/store/00000000000000000000000000000000-hello";
            storePaths = "/nix/store/00000000000000000000000000000000-missing";
          };
        })
        (sleeper "sleep")
        (pod {
          name = "writable";
          args = [ (lib.getExe hello) ];
          readOnly = false;
        })
      ];
    };
    systemd.services.versitygw = {
      wantedBy = [ "multi-user.target" ];
      environment = {
        ROOT_ACCESS_KEY = "nix";
        ROOT_SECRET_KEY = "secret";
      };
      # versitygw keeps metadata in xattrs, which the VM's store lacks.
      serviceConfig.ExecStart = "${lib.getExe versitygw} --port 127.0.0.1:9000 posix --nometa ${fixtures}";
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
    import json

    plugin = "kubectl -n kube-system logs daemonset/nix-store-csi-node -c nix-store-csi"
    volumes = "grep 'kubernetes.io~csi/nix/mount' /proc/mounts"
    views = "ls /var/lib/nix-store-csi/views"
    fetches = f"{plugin} | grep -c ' INFO .*fetching NAR' || true"

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

    with subtest("the admission policy refuses the driver's volumes outside default"):
        machine.succeed("kubectl create namespace other")
        create = "kubectl -n other create --dry-run=server -f"
        # Waits for the namespace's service account and for the API server to
        # load the policy. A dry run creates nothing, so a retry can't fail on
        # a pod that an early try created.
        machine.wait_until_succeeds(f"({create} ${other} 2>&1 || true) | grep -q 'only in these namespaces: default'", timeout=60)
        machine.succeed(f"{create} ${plain true}", f"{create} ${plain false}")
        machine.succeed("kubectl create --dry-run=server -f ${other}")

    with subtest("both pods run, and the plugin fetches each NAR once"):
        phase("hello", "Succeeded")
        t.assertEqual(machine.succeed("kubectl logs hello").strip(), "Hello, world!")
        phase("sleep", "Running")
        t.assertEqual(int(machine.succeed(fetches)), int(machine.succeed("wc -l < ${podsClosure}")))
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
            float(machine.succeed("wc -l < ${podsClosure}")),
        )

    with subtest("kubelet unpublishes hello's volume once hello exits"):
        machine.wait_until_succeeds(f"[ $({volumes} | wc -l) = 1 ]", timeout=60)
        machine.wait_until_succeeds(f"[ $({views} | wc -l) = 1 ]", timeout=60)

    closure = sorted(machine.succeed("xargs -n1 basename < ${writeClosure [ busybox ]}").split())
    ls = "kubectl exec sleep -- ${busybox}/bin/ls /nix/store"

    with subtest("the pod sees only its closure, on one read-only mount"):
        t.assertEqual(sorted(machine.succeed(ls).split()), closure)
        t.assertEqual(machine.succeed("kubectl exec sleep -- ${busybox}/bin/stat -c %a /nix/store").strip(), "755")
        t.assertEqual(machine.succeed(f"{ls}/.roots").strip(), "${busybox.name}")
        [mount] = machine.succeed(volumes).splitlines()
        target, options = mount.split()[1], mount.split()[3].split(",")
        for option in ["ro", "nosuid", "nodev"]:
            t.assertIn(option, options)
        [mount] = machine.succeed("kubectl exec sleep -- ${busybox}/bin/grep ' /nix/store ' /proc/mounts").splitlines()
        t.assertIn("ro", mount.split()[3].split(","))

    with subtest("the runner image gives nobody a home it can write to"):
        machine.succeed("kubectl exec sleep -- ${busybox}/bin/sh -c 'test \"$HOME\" = /home/nobody && ${busybox}/bin/touch \"$HOME\"/x'")

    with subtest("kubelet reports the NAR size and inodes of the pod's closure"):
        summary = "kubectl get --raw /api/v1/nodes/machine/proxy/stats/summary"
        nix = '.pods[] | select(.podRef.name == "sleep") | .volume[]? | select(.name == "nix")'
        machine.wait_until_succeeds(f"{summary} | ${jq}/bin/jq -e '{nix}'", timeout=180)
        stats = json.loads(machine.succeed(f"{summary} | ${jq}/bin/jq '{nix}'"))
        nar_size = sum(
            int(machine.succeed(f"sed -n 's/^NarSize: //p' ${fixtures}/cache-zstd/{name[:32]}.narinfo"))
            for name in closure
        )
        inodes = int(machine.succeed(f"find {target} | wc -l"))
        t.assertEqual([stats[k] for k in ["usedBytes", "capacityBytes", "availableBytes"]], [nar_size, nar_size, 0])
        t.assertEqual([stats[k] for k in ["inodesUsed", "inodes", "inodesFree"]], [inodes, inodes, 0])

    with subtest("a restarted kubelet republishes over another mount at the target"):
        machine.succeed(f"umount {target} && mkdir /tmp/other && mount --bind /tmp/other {target}")
        machine.succeed("systemctl restart k3s")
        machine.wait_until_succeeds(f"{volumes} | grep -q ' ro,'", timeout=300)
        machine.succeed(f"[ $({volumes} | wc -l) = 1 ]")
        t.assertEqual(sorted(machine.succeed(f"ls {target}").split()), closure)

    with subtest("the plugin prefetches the store paths in its ConfigMap as kubelet updates it"):
        def prefetched(revision):
            line = 'nix_store_csi_prefetched{revision="' + revision + '"} 1'
            return f"${curl}/bin/curl -sf http://127.0.0.1:9809/metrics | grep -qxF '{line}'"
        revision = machine.succeed(
            "kubectl -n kube-system get configmap nix-store-csi-prefetch -o jsonpath='{.data.revision}'"
        )
        machine.wait_until_succeeds(prefetched(revision), timeout=60)
        machine.succeed(f"{plugin} | grep 'prefetched store_paths=.*-${busybox.name}' > /dev/null")
        before = int(machine.succeed(fetches))
        machine.succeed("kubectl -n kube-system patch configmap nix-store-csi-prefetch --type merge --patch-file ${prefetchWarm}")
        machine.wait_until_succeeds(prefetched("warm"), timeout=60)
        # busybox, which warm references, is on the node already.
        t.assertEqual(int(machine.succeed(fetches)), before + 1)

    with subtest("the plugin fetches a closure from an S3 bucket"):
        machine.wait_for_open_port(9000)
        store = machine.succeed(
            "AWS_ACCESS_KEY_ID=nix AWS_SECRET_ACCESS_KEY=secret ${lib.getExe nix-store-csi}"
            " --substituters 's3://cache-zstd?endpoint=http://127.0.0.1:9000'"
            " --trusted-public-keys \"$(cat ${fixtures}/public.key)\""
            " --state-dir /tmp/s3 unpack ${hello}"
        ).strip()
        machine.succeed(f"diff -r {store}/${baseNameOf hello} ${hello}")
        t.assertEqual(int(machine.succeed("ls /tmp/s3/store | wc -l")), int(machine.succeed("wc -l < ${writeClosure [ hello ]}")))

    with subtest("a restarted plugin publishes a closure already on the node while the cache is down"):
        machine.succeed("systemctl stop nginx")
        machine.succeed("kubectl -n kube-system delete pod -l app.kubernetes.io/name=nix-store-csi --wait")
        machine.succeed("kubectl apply -f ${offline}")
        phase("offline", "Running")
        t.assertEqual(int(machine.succeed(fetches)), 0)

    with subtest("a pod whose closure the plugin prefetched starts while the cache is down"):
        machine.succeed("kubectl apply -f ${warm}")
        phase("warm", "Succeeded")
        t.assertEqual(machine.succeed("kubectl logs warm").strip(), "warm")

    with subtest("deleting the pods unpublishes their volumes"):
        machine.wait_until_succeeds("kubectl delete pod sleep offline warm --wait --ignore-not-found", timeout=120)
        machine.wait_until_fails(volumes)
        machine.wait_until_succeeds(f"[ -z \"$({views})\" ]", timeout=60)

    with subtest("a node that joins a pool gets the pool's plugin, which prefetches the pool's list"):
        machine.succeed("kubectl label node machine pool=warm")
        pool = "kubectl -n kube-system get pod -l nix-store-csi/pool=warm"
        machine.wait_until_succeeds(f"{pool} -o jsonpath='{{.items[*].status.containerStatuses[*].ready}}' | grep -qx 'true true true'", timeout=300)
        machine.wait_until_succeeds("[ $(kubectl -n kube-system get pod -l app.kubernetes.io/name=nix-store-csi -o name | wc -l) = 1 ]", timeout=120)
        revision = machine.succeed(
            "kubectl -n kube-system get configmap nix-store-csi-prefetch-warm -o jsonpath='{.data.revision}'"
        )
        # The cache is still down, and warm is on the node already.
        machine.wait_until_succeeds(prefetched(revision), timeout=180)
  '';
}
