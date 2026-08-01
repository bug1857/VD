# EXP-001 pre-run resource and workload snapshot

Captured: `2026-08-01T14:25:06Z` (`2026-08-01 19:55:06 +05:30`)

Status: **PRE-RUN ONLY — NOT A BENCHMARK RUN**. No Milvus collection or search operation was performed. A post-run snapshot does not exist yet.

## Background-workload disclosure

Background workloads were **running and were not disabled**. The host was not quiescent. The snapshot showed macOS services, WindowServer, metadata/device-management services, the Docker/Virtualization VM, ChatGPT/Codex processes, assistant services, and other system daemons. Consequently, this snapshot does not authorize latency interpretation; workloads must be stabilized or disclosed again immediately before EXP-002 execution.

```text
uptime: 12 days, 7:41
load averages: 2.61 3.40 4.36
top observed CPU consumers:
WindowServer 39.0%
duetexpertd 26.1%
mdmclient 14.9%
Apple Virtualization VM 13.0%
ChatGPT 13.0%
Codex Renderer 10.8%
```

## Host memory snapshot

```text
physical memory: 8,589,934,592 bytes
page size: 16,384 bytes
pages free: 3,862
pages purgeable: 12
swapins: 3,857,097
swapouts: 5,376,217
```

## Container health and resources

All three containers reported `running` and `healthy`. Images, quotas, and usage were captured through `docker compose ps`, `docker stats --no-stream`, and `docker inspect`.

| Container | CPU usage | Memory usage / limit | Memory % | PIDs | Health | Image ID | CPU quota | Memory limit bytes |
|---|---:|---:|---:|---:|---|---|---:|---:|
| `milvus-standalone` | 8.00% | 328.4 MiB / 4 GiB | 8.02% | 56 | healthy | `sha256:49371c30af46b1013e4d3e0b980e691d81376d69cdbe1b372725baf1d7255862` | 4,000,000,000 NanoCPUs | 4,294,967,296 |
| `milvus-etcd` | 1.16% | 18.79 MiB / 512 MiB | 3.67% | 12 | healthy | `sha256:52f17f7e56e4f7239f0320dbfcbcc24721163d7d78ae710b466af3254ccf6366` | 1,000,000,000 NanoCPUs | 536,870,912 |
| `milvus-minio` | 0.19% | 119.4 MiB / 1 GiB | 11.66% | 11 | healthy | `sha256:391d1d45fdbe79944cb6de9337b073864bb9ee38c4c24280bfb39572e925af08` | 1,000,000,000 NanoCPUs | 1,073,741,824 |

## Git state at evidence capture

```text
commit: 417dfeb52562bf259e02c38fbb0ef3bb94dac319
branch: main (synchronized with origin/main)
working tree: DIRTY
porcelain output: ?? artifacts/exp-001/environment/volumes/
```

The dirty state came from experiment runtime volume contents, not tracked source changes. A future EXP-002 invocation must capture its own execution commit and clean/dirty state in the immutable run manifest.
