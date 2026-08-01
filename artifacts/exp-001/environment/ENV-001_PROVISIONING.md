# ENV-001 provisioning evidence

Captured: `2026-08-01T11:54:39Z`

Outcome: Docker Desktop and the Milvus standalone stack are provisioned and runtime-conformant with ENV-001. All three stock service health checks pass, and a fixed Milvus record persisted across a full `docker compose down` / `up` cycle. No benchmark harness code was written and no benchmark was run.

Formal ENV-001 status was changed to VERIFIED after the persistence and post-restart health gates passed on 2026-08-01. The probe collection remains in the ENV-001 evidence volumes; EXP-001 must use new, explicitly empty experiment-scoped volumes.

## Docker Desktop installation

Installer source: `https://desktop.docker.com/mac/main/arm64/234817/Docker.dmg`

Checksum source: `https://desktop.docker.com/mac/main/arm64/234817/checksums.txt`

```console
$ curl --fail --location --silent --show-error https://desktop.docker.com/mac/main/arm64/234817/Docker.dmg --output /tmp/vd-env001-docker-4.84.0.uiHrqb/Docker.dmg
$ curl --fail --location --silent --show-error https://desktop.docker.com/mac/main/arm64/234817/checksums.txt --output /tmp/vd-env001-docker-4.84.0.uiHrqb/checksums.txt
$ shasum -a 256 /tmp/vd-env001-docker-4.84.0.uiHrqb/Docker.dmg
ed9e93bf2b71c53492eb80ef35e722e131222018cba8157973dfe3bb717952dd  /tmp/vd-env001-docker-4.84.0.uiHrqb/Docker.dmg
$ sed -n 1p /tmp/vd-env001-docker-4.84.0.uiHrqb/checksums.txt
ed9e93bf2b71c53492eb80ef35e722e131222018cba8157973dfe3bb717952dd *Docker.dmg
$ hdiutil attach /tmp/vd-env001-docker-4.84.0.uiHrqb/Docker.dmg -nobrowse -readonly
Checksumming Protective Master Boot Record (MBR : 0)…
Protective Master Boot Record (MBR :: verified CRC32 $536CEC2D
Checksumming GPT Header (Primary GPT Header : 1)…
 GPT Header (Primary GPT Header : 1): verified CRC32 $B6E25F88
Checksumming GPT Partition Data (Primary GPT Table : 2)…
GPT Partition Data (Primary GPT Tabl: verified CRC32 $014DA265
Checksumming  (Apple_Free : 3)…
                    (Apple_Free : 3): verified CRC32 $00000000
Checksumming EFI System Partition (C12A7328-F81F-11D2-BA4B-00A0C93EC93B : 4)…
EFI System Partition (C12A7328-F81F-: verified CRC32 $B54B659C
Checksumming disk image (Apple_HFS : 5)…
          disk image (Apple_HFS : 5): verified CRC32 $C5628405
Checksumming  (Apple_Free : 6)…
                    (Apple_Free : 6): verified CRC32 $00000000
Checksumming GPT Partition Data (Backup GPT Table : 7)…
GPT Partition Data (Backup GPT Table: verified CRC32 $014DA265
Checksumming GPT Header (Backup GPT Header : 8)…
  GPT Header (Backup GPT Header : 8): verified CRC32 $48C54FE6
verified CRC32 $88771973
/dev/disk4          GUID_partition_scheme
/dev/disk4s1        EFI
/dev/disk4s2        Apple_HFS                       /Volumes/Docker
$ /usr/libexec/PlistBuddy -c Print:CFBundleShortVersionString /Volumes/Docker/Docker.app/Contents/Info.plist
4.84.0
$ /usr/libexec/PlistBuddy -c Print:CFBundleVersion /Volumes/Docker/Docker.app/Contents/Info.plist
234817
$ ditto /Volumes/Docker/Docker.app /Applications/Docker.app
$ hdiutil detach /Volumes/Docker
"disk4" ejected.
$ open -a /Applications/Docker.app
$ wait for docker info
docker_ready_after_seconds=6
ServerVersion=29.6.2 OperatingSystem=Docker Desktop Architecture=aarch64 CPUs=8 MemoryBytes=4108632064
```

The ENV-001 VM settings were applied in Docker Desktop's `settings-store.json`, then Docker Desktop was restarted:

```console
$ docker desktop stop
✓ Stopping Docker Desktop
$ settings-store resource keys
  "CPUs": 6,
  "MemoryMiB": 6144,
  "SwapMiB": 2048,
$ docker desktop start
✓ Starting Docker Desktop
$ wait for Docker after resource-settings restart
docker_ready_after_seconds=2
ServerVersion=29.6.2 OperatingSystem=Docker Desktop Architecture=aarch64 CPUs=6 MemoryBytes=6212349952
Docker Compose version v5.3.1
$ Docker Desktop settings-store normalized resource pins
  "Cpus": 6,
  "MemoryMiB": 6144,
  "SwapMiB": 2048,
```

## Vendor Compose fetch and authorized change set

```console
$ curl --fail --location --silent --show-error https://github.com/milvus-io/milvus/releases/download/v3.0.0/milvus-standalone-docker-compose.yml --output /tmp/vd-env001-compose/milvus-standalone-docker-compose.yml
$ shasum -a 256 /tmp/vd-env001-compose/milvus-standalone-docker-compose.yml
4518b95ddd719542558f48d84e9a53a5910099888b8ef985ab122524db7d97d1  /tmp/vd-env001-compose/milvus-standalone-docker-compose.yml
$ expected SHA-256 from ENV-001
4518b95ddd719542558f48d84e9a53a5910099888b8ef985ab122524db7d97d1
$ image declarations from fetched asset
    image: quay.io/coreos/etcd:v3.5.25
    image: minio/minio:RELEASE.2024-05-28T17-19-04Z
    image: milvusdb/milvus:v3.0.0
$ shasum -a 256 infra/milvus/env-001/compose.vendor.yml
4518b95ddd719542558f48d84e9a53a5910099888b8ef985ab122524db7d97d1  infra/milvus/env-001/compose.vendor.yml
$ diff -u /tmp/vd-env001-compose/milvus-standalone-docker-compose.yml infra/milvus/env-001/compose.vendor.yml
$ shasum -a 256 infra/milvus/env-001/compose.vendor.yml infra/milvus/env-001/compose.override.yml
4518b95ddd719542558f48d84e9a53a5910099888b8ef985ab122524db7d97d1  infra/milvus/env-001/compose.vendor.yml
bd97b91052ac642593c0af33aa7e90519e472a168d4ada48ba71f0846a4ee8c6  infra/milvus/env-001/compose.override.yml
$ docker compose -p vd-exp001 -f infra/milvus/env-001/compose.vendor.yml -f infra/milvus/env-001/compose.override.yml config | shasum -a 256
time="2026-08-01T17:26:19+05:30" level=warning msg="/Users/rudrapratapsingh/Desktop/VD/infra/milvus/env-001/compose.vendor.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion"
76310aee683a1dab714679f0f9202bc193ad87019e2e8bbf3c25fb46454ea217  -
```

The `version` warning comes from the untouched vendor asset. Removing it would violate the byte-identical vendor-copy requirement.

Effective authorized fields:

```console
$ docker compose config (effective images, limits, mounts)
name: vd-exp001
services:
  etcd:
    cpus: 1
    image: quay.io/coreos/etcd:v3.5.25@sha256:52f17f7e56e4f7239f0320dbfcbcc24721163d7d78ae710b466af3254ccf6366
    mem_limit: "536870912"
  minio:
    cpus: 1
    image: minio/minio:RELEASE.2024-05-28T17-19-04Z@sha256:391d1d45fdbe79944cb6de9337b073864bb9ee38c4c24280bfb39572e925af08
    mem_limit: "1073741824"
  standalone:
    cpus: 4
    image: milvusdb/milvus:v3.0.0@sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862
    mem_limit: "4294967296"
$ effective bind mounts
source: /Users/rudrapratapsingh/Desktop/VD/artifacts/exp-001/environment/volumes/etcd
target: /etcd
source: /Users/rudrapratapsingh/Desktop/VD/artifacts/exp-001/environment/volumes/minio
target: /minio_data
source: /Users/rudrapratapsingh/Desktop/VD/artifacts/exp-001/environment/volumes/milvus
target: /var/lib/milvus
```

## Pull and start

The initial pull completed `35/35`; the compact post-pull verification output is retained here without terminal animation frames:

```console
$ docker compose -p vd-exp001 -f infra/milvus/env-001/compose.vendor.yml -f infra/milvus/env-001/compose.override.yml pull --quiet
time="2026-08-01T17:26:26+05:30" level=warning msg="/Users/rudrapratapsingh/Desktop/VD/infra/milvus/env-001/compose.vendor.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion"
 Image quay.io/coreos/etcd:v3.5.25@sha256:52f17f7e56e4f7239f0320dbfcbcc24721163d7d78ae710b466af3254ccf6366 Pulling
 Image minio/minio:RELEASE.2024-05-28T17-19-04Z@sha256:391d1d45fdbe79944cb6de9337b073864bb9ee38c4c24280bfb39572e925af08 Pulling
 Image milvusdb/milvus:v3.0.0@sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862 Pulling
 Image quay.io/coreos/etcd:v3.5.25@sha256:52f17f7e56e4f7239f0320dbfcbcc24721163d7d78ae710b466af3254ccf6366 Pulled
 Image milvusdb/milvus:v3.0.0@sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862 Pulled
 Image minio/minio:RELEASE.2024-05-28T17-19-04Z@sha256:391d1d45fdbe79944cb6de9337b073864bb9ee38c4c24280bfb39572e925af08 Pulled
$ docker compose -p vd-exp001 -f infra/milvus/env-001/compose.vendor.yml -f infra/milvus/env-001/compose.override.yml up -d
Network milvus Creating
Network milvus Created
Container milvus-minio Creating
Container milvus-etcd Creating
Container milvus-etcd Created
Container milvus-minio Created
Container milvus-standalone Creating
Container milvus-standalone Created
Container milvus-minio Starting
Container milvus-etcd Starting
Container milvus-minio Started
Container milvus-etcd Started
Container milvus-standalone Starting
Container milvus-standalone Started
$ docker compose up -d (idempotence verification)
time="2026-08-01T17:26:37+05:30" level=warning msg="/Users/rudrapratapsingh/Desktop/VD/infra/milvus/env-001/compose.vendor.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion"
Container milvus-standalone Running
Container milvus-etcd Running
Container milvus-minio Running
```

## Health checks

```console
$ wait for all stock health checks (up to 240 seconds)
t=+010s milvus-etcd=starting milvus-minio=starting milvus-standalone=healthy
t=+020s milvus-etcd=starting milvus-minio=starting milvus-standalone=healthy
t=+030s milvus-etcd=healthy milvus-minio=healthy milvus-standalone=healthy
$ docker exec milvus-etcd etcdctl endpoint health
127.0.0.1:2379 is healthy: successfully committed proposal: took = 1.663375ms
$ docker exec milvus-minio mc ready local
The cluster is ready
$ docker exec milvus-standalone curl -f http://localhost:9091/healthz
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0
100     2  100     2    0     0   4618      0 --:--:-- --:--:-- --:--:--  2000
OK
```

## Runtime versions

Milvus does not implement a `milvus --version` command. Its live process exposes build identity through the standard metrics endpoint.

```console
$ /usr/libexec/PlistBuddy -c Print:CFBundleShortVersionString /Applications/Docker.app/Contents/Info.plist
4.84.0
$ docker version
Client:
 Version:           29.6.2
 API version:       1.55
 Go version:        go1.26.5
 Git commit:        dfc4efb
 Built:             Thu Jul 16 16:11:17 2026
 OS/Arch:           darwin/arm64
 Context:           desktop-linux

Server: Docker Desktop 4.84.0 (234817)
 Engine:
  Version:          29.6.2
  API version:      1.55 (minimum version 1.40)
  Go version:       go1.26.5
  Git commit:       3d80467
  Built:            Thu Jul 16 16:13:03 2026
  OS/Arch:          linux/arm64
  Experimental:     false
 containerd:
  Version:          v2.2.5
  GitCommit:        e53c7c1516c3b2bff98eb76f1f4117477e6f4e66
 runc:
  Version:          1.3.6
  GitCommit:        v1.3.6-0-g491b69ba
 docker-init:
  Version:          0.19.0
  GitCommit:        de40ad0
$ docker compose version
Docker Compose version v5.3.1
$ curl -fsS http://localhost:9091/metrics | rg milvus_build_info
# HELP milvus_build_info Build information of milvus
# TYPE milvus_build_info gauge
milvus_build_info{built="Wed Jul 29 11:12:18 UTC 2026",git_commit="f46a032855",version="3.0.0"} 1
$ docker exec milvus-etcd etcd --version
etcd Version: 3.5.25
Git SHA: e2eff77
Go Version: go1.24.10
Go OS/Arch: linux/arm64
$ docker exec milvus-minio minio --version
minio version RELEASE.2024-05-28T17-19-04Z (commit-id=f79a4ef4d0dc3e6562cad0d1d1db674bc8c75531)
Runtime: go1.22.3 linux/arm64
License: GNU AGPLv3 - https://www.gnu.org/licenses/agpl-3.0.html
Copyright: 2015-2024 MinIO, Inc.
```

## Resolved immutable digests

```console
$ docker buildx imagetools inspect Milvus (index + linux/arm64 manifest)
index=sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862 arm64=sha256:bfab7739a0479cd81ffdf5e473f88c5b143678c2520a06a19f86f35ecd586cad
$ docker buildx imagetools inspect etcd (index + linux/arm64 manifest)
index=sha256:52f17f7e56e4f7239f0320dbfcbcc24721163d7d78ae710b466af3254ccf6366 arm64=sha256:8da34a9df5dc1bd879bea716a301113c4e49b6bbdbe5778214707c6043ccf65d
$ docker buildx imagetools inspect MinIO (index + linux/arm64 manifest)
index=sha256:391d1d45fdbe79944cb6de9337b073864bb9ee38c4c24280bfb39572e925af08 arm64=sha256:fa7be14ee3f914469274c5dfc05949e0092500a71de4681f1f1b6b39275a13b1
$ local image IDs, architecture, RepoDigests
Id=sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862 Architecture=arm64 RepoDigests=["milvusdb/milvus@sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862"]
Id=sha256:52f17f7e56e4f7239f0320dbfcbcc24721163d7d78ae710b466af3254ccf6366 Architecture=arm64 RepoDigests=["quay.io/coreos/etcd@sha256:52f17f7e56e4f7239f0320dbfcbcc24721163d7d78ae710b466af3254ccf6366"]
Id=sha256:391d1d45fdbe79944cb6de9337b073864bb9ee38c4c24280bfb39572e925af08 Architecture=arm64 RepoDigests=["minio/minio@sha256:391d1d45fdbe79944cb6de9337b073864bb9ee38c4c24280bfb39572e925af08"]
```

## Pre-run resource snapshot

```console
$ date -u +%Y-%m-%dT%H:%M:%SZ
2026-08-01T11:54:39Z
$ uname -m && sw_vers
arm64
ProductName:        macOS
ProductVersion:     26.5.2
BuildVersion:       25F84
$ host CPU and RAM
hw.ncpu=8
hw.physicalcpu=8
hw.logicalcpu=8
hw.memsize=8589934592
$ Docker Desktop VM allocation from daemon
ServerVersion=29.6.2 OS=Docker Desktop Architecture=aarch64 CPUs=6 MemoryBytes=6212349952
$ container hard limits
/milvus-etcd NanoCpus=1000000000 MemoryBytes=536870912 MemorySwapBytes=1073741824
/milvus-minio NanoCpus=1000000000 MemoryBytes=1073741824 MemorySwapBytes=2147483648
/milvus-standalone NanoCpus=4000000000 MemoryBytes=4294967296 MemorySwapBytes=8589934592
$ docker stats --no-stream
NAME                CPU %     MEM USAGE / LIMIT   MEM %     NET I/O           BLOCK I/O     PIDS
milvus-etcd         0.59%     18.31MiB / 512MiB   3.58%     194kB / 151kB     0B / 0B       12
milvus-minio        0.24%     120.2MiB / 1GiB     11.74%    31.9kB / 14.5kB   0B / 12.3kB   12
milvus-standalone   4.94%     279.9MiB / 4GiB     6.83%     221kB / 2.46MB    0B / 20.5kB   52
$ host vm_stat (pre-run)
Mach Virtual Memory Statistics: (page size of 16384 bytes)
Pages free:                                     4531.
Pages active:                                  94902.
Pages inactive:                                91045.
Pages speculative:                              3380.
Pages throttled:                                   0.
Pages wired down:                              96363.
Pages purgeable:                                   4.
"Translation faults":                     2678730997.
Pages copy-on-write:                        45539919.
Pages zero filled:                        2328624934.
Pages reactivated:                         666201230.
```

## Persistence-across-restart verification

Captured: `2026-08-01T12:06:06Z`

Probe contract:

- Collection: `env001_persistence_probe_20260801`
- Primary key: `1001`
- Marker: `ENV001-PERSIST-20260801-001`
- Vector: `[0.125, 0.25, 0.5, 1.0]`
- Pass condition: the exact primary key and marker are queryable before and after a full Compose teardown/recreation, followed by all three stock health checks reporting healthy.

### Write and pre-restart read

```console
$ curl -sS -X POST http://localhost:19530/v2/vectordb/collections/list -H 'Content-Type: application/json' -d '{}'
{"code":0,"data":[]}
$ curl --fail-with-body -sS -X POST http://localhost:19530/v2/vectordb/collections/create \
  -H 'Content-Type: application/json' \
  -d '{"collectionName":"env001_persistence_probe_20260801","dimension":4,"metricType":"COSINE","primaryFieldName":"id","vectorFieldName":"vector"}'
{"code":0,"data":{}}
$ curl --fail-with-body -sS -X POST http://localhost:19530/v2/vectordb/entities/insert \
  -H 'Content-Type: application/json' \
  -d '{"collectionName":"env001_persistence_probe_20260801","data":[{"id":1001,"vector":[0.125,0.25,0.5,1.0],"marker":"ENV001-PERSIST-20260801-001"}]}'
{"code":0,"cost":0,"data":{"insertCount":1,"insertIds":[1001]}}
$ curl --fail-with-body -sS -X POST http://localhost:19530/v2/vectordb/entities/query \
  -H 'Content-Type: application/json' \
  -d '{"collectionName":"env001_persistence_probe_20260801","filter":"id == 1001","outputFields":["id","marker"]}'
{"code":0,"cost":0,"data":[{"id":1001,"marker":"ENV001-PERSIST-20260801-001"}]}
```

### Full stack teardown and recreation

```console
$ export ENV001_VOLUME_ROOT=/Users/rudrapratapsingh/Desktop/VD/artifacts/exp-001/environment/volumes
$ docker compose -p vd-exp001 \
  -f infra/milvus/env-001/compose.vendor.yml \
  -f infra/milvus/env-001/compose.override.yml \
  down
time="2026-08-01T17:34:56+05:30" level=warning msg="/Users/rudrapratapsingh/Desktop/VD/infra/milvus/env-001/compose.vendor.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion"
 Container milvus-standalone Stopping
 Container milvus-standalone Stopped
 Container milvus-standalone Removing
 Container milvus-standalone Removed
 Container milvus-etcd Stopping
 Container milvus-minio Stopping
 Container milvus-minio Stopped
 Container milvus-minio Removing
 Container milvus-minio Removed
 Container milvus-etcd Stopped
 Container milvus-etcd Removing
 Container milvus-etcd Removed
 Network milvus Removing
 Network milvus Removed
$ docker ps -a --filter name=milvus --format 'table {{.Names}}\t{{.Status}}'
NAMES     STATUS
$ docker compose -p vd-exp001 \
  -f infra/milvus/env-001/compose.vendor.yml \
  -f infra/milvus/env-001/compose.override.yml \
  up -d
time="2026-08-01T17:34:58+05:30" level=warning msg="/Users/rudrapratapsingh/Desktop/VD/infra/milvus/env-001/compose.vendor.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion"
 Network milvus Creating
 Network milvus Created
 Container milvus-etcd Creating
 Container milvus-minio Creating
 Container milvus-minio Created
 Container milvus-etcd Created
 Container milvus-standalone Creating
 Container milvus-standalone Created
 Container milvus-minio Starting
 Container milvus-etcd Starting
 Container milvus-etcd Started
 Container milvus-minio Started
 Container milvus-standalone Starting
 Container milvus-standalone Started
```

### Post-restart health and persisted read

```console
$ wait for all stock health checks after restart (up to 240 seconds)
t=+010s milvus-etcd=starting milvus-minio=starting milvus-standalone=starting
t=+020s milvus-etcd=starting milvus-minio=starting milvus-standalone=healthy
t=+030s milvus-etcd=starting milvus-minio=starting milvus-standalone=healthy
t=+040s milvus-etcd=healthy milvus-minio=healthy milvus-standalone=healthy
$ curl --fail-with-body -sS -X POST http://localhost:19530/v2/vectordb/collections/list \
  -H 'Content-Type: application/json' \
  -d '{}'
{"code":0,"data":["env001_persistence_probe_20260801"]}
$ curl --fail-with-body -sS -X POST http://localhost:19530/v2/vectordb/entities/query \
  -H 'Content-Type: application/json' \
  -d '{"collectionName":"env001_persistence_probe_20260801","filter":"id == 1001","outputFields":["id","marker"]}'
{"code":0,"cost":0,"data":[{"id":1001,"marker":"ENV001-PERSIST-20260801-001"}]}
$ docker exec milvus-etcd etcdctl endpoint health
127.0.0.1:2379 is healthy: successfully committed proposal: took = 1.113875ms
$ docker exec milvus-minio mc ready local
The cluster is ready
$ docker exec milvus-standalone curl -fsS http://localhost:9091/healthz
OK
$ docker compose -p vd-exp001 \
  -f infra/milvus/env-001/compose.vendor.yml \
  -f infra/milvus/env-001/compose.override.yml \
  ps
time="2026-08-01T17:35:53+05:30" level=warning msg="/Users/rudrapratapsingh/Desktop/VD/infra/milvus/env-001/compose.vendor.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion"
NAME                IMAGE                                                                                                              COMMAND                  SERVICE      CREATED          STATUS                    PORTS
milvus-etcd         quay.io/coreos/etcd:v3.5.25@sha256:52f17f7e56e4f7239f0320dbfcbcc24721163d7d78ae710b466af3254ccf6366                "etcd -advertise-cli…"   etcd         55 seconds ago   Up 54 seconds (healthy)   2379-2380/tcp
milvus-minio        minio/minio:RELEASE.2024-05-28T17-19-04Z@sha256:391d1d45fdbe79944cb6de9337b073864bb9ee38c4c24280bfb39572e925af08   "/usr/bin/docker-ent…"   minio        55 seconds ago   Up 54 seconds (healthy)   0.0.0.0:9000-9001->9000-9001/tcp, [::]:9000-9001->9000-9001/tcp
milvus-standalone   milvusdb/milvus:v3.0.0@sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862                     "/tini -- milvus run…"   standalone   55 seconds ago   Up 54 seconds (healthy)   0.0.0.0:9091->9091/tcp, [::]:9091->9091/tcp, 0.0.0.0:19530->19530/tcp, [::]:19530->19530/tcp
```

Result: **PASS**. The exact record survived container and network removal/recreation through the unchanged experiment-scoped Milvus, etcd, and MinIO bind volumes, and every stock service health check returned healthy afterward.

## Rollback / stop command

This stops the stack while retaining the experiment-scoped data:

```sh
ENV001_VOLUME_ROOT=/Users/rudrapratapsingh/Desktop/VD/artifacts/exp-001/environment/volumes \
docker compose -p vd-exp001 \
  -f infra/milvus/env-001/compose.vendor.yml \
  -f infra/milvus/env-001/compose.override.yml \
  down
```
