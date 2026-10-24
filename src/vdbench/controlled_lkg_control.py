"""ADR-026 closed control observations and gap-intolerant M/K/Q coverage.

Parsing and state-machine checks are offline. A transport must supply actual
native snapshots/watch responses; a profile or test response is not live proof.
No Docker configuration, service/client or background thread is created here.
"""

from __future__ import annotations

import hashlib
import ipaddress
import posixpath
import re
from functools import wraps
from collections.abc import Callable, Mapping
from time import monotonic_ns
from typing import Any

from .canonical_serialization import strict_canonical_json_bytes
from .controlled_rematerialization import RematerializationError, digest, require

MAINTENANCE_SETTINGS = (
    "dataCoord.enableCompaction", "dataCoord.autoUpgradeSegmentIndex",
    "dataCoord.forceRebuildSegmentIndex", "dataCoord.forceRebuildScalarSegmentIndex",
    "dataCoord.enableGarbageCollection",
)
LIMITS = {"snapshot_keys": 100000, "snapshot_bytes": 128 * 1024 * 1024,
          "pending_events": 16384, "pending_bytes": 32 * 1024 * 1024,
          "journal_bytes": 256 * 1024 * 1024, "progress_interval_ns": 1000000000,
          "progress_deadline_ns": 10000000000}
MEMBERS = {
    "qualification": {"worker", "qualification_gateway"},
    "observer": {"observer", "observer_milvus_gateway", "observer_etcd_gateway"},
    "backend": {"milvus", "etcd", "minio", "qualification_gateway",
                "observer_milvus_gateway", "observer_etcd_gateway"},
}
GATEWAYS = {
    "qualification_gateway": ("qualification", "worker", "milvus", 19530),
    "observer_milvus_gateway": ("observer", "observer", "milvus", 19530),
    "observer_etcd_gateway": ("observer", "observer", "etcd", 2379),
}
TASK_FAMILIES = (
    "index", "index_recovery", "compaction", "stats", "analyze", "preimport",
    "import", "copy_segment", "snapshot", "restore", "external_refresh",
    "replication_cdc", "ingestion_delete_flush_wal", "ddl_load_target_balance", "gc",
)
_HEX = re.compile(r"[0-9a-f]{64}\Z")


def _text(value: object) -> str:
    require(type(value) is str and 0 < len(value) <= 256
            and value.isascii() and all(32 <= ord(c) < 127 for c in value),
            "CONTROL_TEXT_INVALID")
    return value  # type: ignore[return-value]


def _shape(value: object, fields: set[str], code: str) -> dict[str, Any]:
    require(type(value) is dict and set(value) == fields, code)
    return value  # type: ignore[return-value]


def _positive(value: object, code: str) -> int:
    require(type(value) is int and value > 0, code)
    return value  # type: ignore[return-value]


def _ip(value: object) -> str:
    value = _text(value)
    address = ipaddress.ip_address(value)
    require(address.version == 4 and address.is_private and not address.is_loopback
            and not address.is_unspecified and not address.is_link_local, "PROFILE_IP_INVALID")
    require(str(address) == value, "PROFILE_IP_INVALID")
    return value


def gateway_configuration(front: str, source: str, backend: str, port: int) -> str:
    """One fixed native stream route; no arbitrary configuration acceptance."""
    front, source, backend = _ip(front), _ip(source), _ip(backend)
    require(type(port) is int and port in (19530, 2379), "GATEWAY_PORT_INVALID")
    return (
        "worker_processes 1;\npid /tmp/nginx.pid;\n"
        "events { worker_connections 1024; }\nstream {\n server {\n"
        f"  listen {front}:{port};\n  allow {source};\n  deny all;\n"
        f"  proxy_pass {backend}:{port};\n  proxy_next_upstream off;\n"
        "  proxy_connect_timeout 5s;\n  proxy_timeout 60s;\n"
        "  proxy_buffer_size 64k;\n }\n}\n"
    )


def validate_profile(value: dict[str, Any]) -> dict[str, Any]:
    """No published ports, alternate network or unbound gateway is acceptable."""
    _shape(value, {"schema_version", "networks", "containers", "gateways",
                   "principals", "maintenance", "etcd", "limits"}, "PROFILE_SHAPE_INVALID")
    require(value["schema_version"] == "controlled-lkg-profile-v1"
            and value["limits"] == LIMITS, "PROFILE_VERSION_OR_BOUNDS_INVALID")
    networks = _shape(value["networks"], set(MEMBERS), "PROFILE_NETWORKS_INVALID")
    for name, members in MEMBERS.items():
        network = _shape(networks[name], {"driver", "internal", "ipv6", "members"},
                         "PROFILE_NETWORK_INVALID")
        require(network["driver"] == "bridge" and network["internal"] is True
                and network["ipv6"] is False and type(network["members"]) is list
                and len(network["members"]) == len(members)
                and set(network["members"]) == members, "PROFILE_MEMBERSHIP_INVALID")
    containers = _shape(value["containers"], set().union(*MEMBERS.values()),
                        "PROFILE_CONTAINERS_INVALID")
    ids: set[str] = set()
    for role, facts in containers.items():
        _shape(facts, {"id", "image", "name", "networks", "mounts", "cpu_nanos",
                       "memory_bytes", "pids_limit"}, "PROFILE_CONTAINER_INVALID")
        require(type(facts["id"]) is str and _HEX.fullmatch(facts["id"]) is not None
                and facts["id"] not in ids, "PROFILE_RUNTIME_ID_INVALID")
        ids.add(facts["id"])
        require(type(facts["image"]) is str
                and re.fullmatch(r"sha256:[0-9a-f]{64}", facts["image"]) is not None,
                "PROFILE_IMAGE_UNPINNED")
        _text(facts["name"])
        expected = {n for n, m in MEMBERS.items() if role in m}
        require(type(facts["networks"]) is dict and set(facts["networks"]) == expected,
                "PROFILE_CONTAINER_MEMBERSHIP_INVALID")
        for ip in facts["networks"].values():
            _ip(ip)
        for limit in ("cpu_nanos", "memory_bytes", "pids_limit"):
            _positive(facts[limit], "PROFILE_RESOURCE_BOUND_INVALID")
        require(type(facts["mounts"]) is list, "PROFILE_MOUNTS_INVALID")
        for mount in facts["mounts"]:
            _shape(mount, {"source", "destination", "read_only", "type"},
                   "PROFILE_MOUNT_INVALID")
            _text(mount["source"])
            _text(mount["destination"])
            require(type(mount["read_only"]) is bool and mount["type"] in {"volume", "bind"},
                    "PROFILE_MOUNT_INVALID")
        if role in GATEWAYS or role == "observer":
            require(facts["mounts"] == [], "GATEWAY_DEPLOYMENT_MOUNT_FORBIDDEN")
        if role == "worker":
            require(len(facts["mounts"]) <= 2
                    and len({m["destination"] for m in facts["mounts"]}) == len(facts["mounts"])
                    and all(m["read_only"] is True and m["type"] == "bind"
                            and m["destination"] in {"/input/dataset-001", "/input/dataset-003"}
                            for m in facts["mounts"]), "WORKER_DEPLOYMENT_MOUNT_FORBIDDEN")
    for network in MEMBERS:
        ips = [f["networks"][network] for f in containers.values()
               if network in f["networks"]]
        require(len(ips) == len(set(ips)), "PROFILE_IP_COLLISION")
    gateways = _shape(value["gateways"], set(GATEWAYS), "PROFILE_GATEWAYS_INVALID")
    for role, (front, initiator, backend, port) in GATEWAYS.items():
        gateway = _shape(gateways[role], {"version", "modules", "configuration"},
                         "PROFILE_GATEWAY_INVALID")
        require(gateway["version"] == "1.30.5" and gateway["modules"] == ["stream", "stream_access"],
                "GATEWAY_SOFTWARE_INVALID")
        configuration = gateway_configuration(
            containers[role]["networks"][front], containers[initiator]["networks"][front],
            containers[backend]["networks"]["backend"], port,
        )
        require(gateway["configuration"] == configuration, "GATEWAY_CONFIGURATION_INVALID")
    principals = _shape(value["principals"], {"materializer", "qualifier", "observer"},
                        "PROFILE_PRINCIPALS_INVALID")
    require(len(set(_text(p) for p in principals.values())) == 3,
            "PROFILE_PRINCIPAL_OVERLAP")
    require(type(value["maintenance"]) is dict
            and set(value["maintenance"]) == set(MAINTENANCE_SETTINGS)
            and all(v is False for v in value["maintenance"].values()),
            "MAINTENANCE_CONFIGURATION_INVALID")
    etcd = _shape(value["etcd"], {"rootPath", "metaSubPath", "kvSubPath"}, "ETCD_ROOTS_INVALID")
    for part in etcd.values():
        _text(part)
    protected_ranges(etcd)
    # Detached, strict bytes are the only retained profile representation.
    import json
    return json.loads(strict_canonical_json_bytes(value))


def validate_origin(value: dict[str, Any], profile: Mapping[str, Any]) -> dict[str, Any]:
    """Empty reads alone are excluded; trusted allocation evidence is required."""
    _shape(value, {"schema_version", "allocation", "identities", "storage_mounts",
                   "restored_from", "prior_materializations", "aliases", "routes"},
           "ORIGIN_SHAPE_INVALID")
    require(value["schema_version"] == "fresh-controlled-origin-v1"
            and value["restored_from"] == [] and value["prior_materializations"] == []
            and value["aliases"] == [] and value["routes"] == [], "ORIGIN_NOT_FRESH")
    allocation = _shape(value["allocation"], {"owner", "request_sha256", "created_at_utc",
                                              "receipt_sha256"}, "ALLOCATION_EVIDENCE_INVALID")
    from .lkg_window_readiness import validate_rfc3339_utc
    _text(allocation["owner"])
    validate_rfc3339_utc(allocation["created_at_utc"], field="created_at_utc")
    for name in ("request_sha256", "receipt_sha256"):
        require(type(allocation[name]) is str and _HEX.fullmatch(allocation[name]) is not None,
                "ALLOCATION_EVIDENCE_INVALID")
    require(type(value["identities"]) is list and len(value["identities"]) >= 6
            and len(set(value["identities"])) == len(value["identities"]),
            "ORIGIN_IDENTITIES_INVALID")
    for identity in value["identities"]:
        _text(identity)
    for role in ("milvus", "etcd", "minio"):
        require("runtime:" + profile["containers"][role]["id"] in value["identities"],
                "ORIGIN_RUNTIME_UNBOUND")
    mounts = _shape(value["storage_mounts"], {"milvus", "etcd", "minio"},
                    "ORIGIN_STORAGE_UNBOUND")
    for role, observed in mounts.items():
        require(type(observed) is list and observed
                and observed == profile["containers"][role]["mounts"],
                "ORIGIN_STORAGE_UNBOUND")
        for mount in observed:
            require("storage:" + digest("storage-mount", mount) in value["identities"],
                    "ORIGIN_STORAGE_UNBOUND")
    import json
    return json.loads(strict_canonical_json_bytes(value))


def validate_runtime(
    observation: dict[str, Any], profile: Mapping[str, Any], *, phase: str,
) -> dict[str, Any]:
    """Validate a closed observation format; this cannot establish provenance.

    These parsed facts alone never issue a production admission capability.
    Actual source-bound probes and queue/executor coverage remain required.
    """
    require(phase in {"construction", "readonly"}, "CONTROL_PHASE_INVALID")
    _shape(observation, {"containers", "networks", "gateways", "maintenance", "rbac",
                         "worker_processes", "sessions", "denials", "ownership"},
           "CONTROL_OBSERVATION_INVALID")
    require(observation["networks"] == profile["networks"]
            and observation["gateways"] == profile["gateways"]
            and observation["maintenance"] == profile["maintenance"], "CONTROL_PROFILE_DRIFT")
    require(observation["ownership"] == {"governance_owner": True, "coverage_owner": True},
            "CONTROL_OWNER_LOST")
    require(type(observation["containers"]) is dict
            and set(observation["containers"]) == set(profile["containers"]),
            "CONTROL_CONTAINER_SET_DRIFT")
    for role, expected in profile["containers"].items():
        raw = _shape(observation["containers"][role], {
            "identity", "published_ports", "privileged", "uid", "read_only_rootfs",
            "capabilities", "no_new_privileges", "host_namespaces", "devices",
            "forwarding", "ipv6_interfaces", "health", "restart_count", "started_at_utc",
        }, "CONTROL_CONTAINER_OBSERVATION_INVALID")
        require(raw["identity"] == expected and raw["published_ports"] == []
                and raw["privileged"] is False and raw["host_namespaces"] == []
                and raw["devices"] == [] and type(raw["restart_count"]) is int
                and raw["restart_count"] == 0 and raw["health"] == "healthy",
                "CONTROL_CONTAINER_UNSAFE")
        from .lkg_window_readiness import validate_rfc3339_utc
        validate_rfc3339_utc(raw["started_at_utc"], field="started_at_utc")
        if role in GATEWAYS or role in ("worker", "observer"):
            _positive(raw["uid"], "CONTROL_PRIVILEGED_UID")
            require(raw["read_only_rootfs"] is True and raw["capabilities"] == []
                    and raw["no_new_privileges"] is True, "CONTROL_CONTAINER_UNSAFE")
        require(type(raw["forwarding"]) is dict
                and all(type(v) is int for v in raw["forwarding"].values())
                and raw["forwarding"] == {
            "net.ipv4.ip_forward": 0, "net.ipv6.conf.all.forwarding": 0,
            "net.ipv6.conf.default.forwarding": 0,
        }, "CONTROL_FORWARDING_UNSAFE")
        require(type(raw["ipv6_interfaces"]) is dict and raw["ipv6_interfaces"]
                and all(type(v) is int and v == 1 for v in raw["ipv6_interfaces"].values()),
                "CONTROL_IPV6_UNCERTAIN")
    rbac = _shape(observation["rbac"], {"auth_enabled", "principals", "effective_writers",
                                      "inherited_mutation_grants", "etcd_permissions",
                                      "service_credentials"}, "CONTROL_RBAC_INVALID")
    require(rbac["auth_enabled"] is True and rbac["principals"] == profile["principals"]
            and rbac["inherited_mutation_grants"] == [] and rbac["service_credentials"] == [],
            "CONTROL_RBAC_UNSAFE")
    expected_writers = [profile["principals"]["materializer"]] if phase == "construction" else []
    require(rbac["effective_writers"] == expected_writers, "COMPETING_OR_STALE_WRITER")
    require(rbac["etcd_permissions"] == {"observer": ["Range", "Watch"]},
            "CONTROL_ETCD_PERMISSION_UNSAFE")
    worker_role = "materializer" if phase == "construction" else "qualifier"
    require(type(observation["worker_processes"]) is list
            and len(observation["worker_processes"]) == 1, "WORKER_PHASE_OVERLAP")
    process = _shape(observation["worker_processes"][0], {"role", "pid", "principal", "started_at_utc"},
                     "WORKER_PROCESS_INVALID")
    require(process["role"] == worker_role and process["principal"] == profile["principals"][worker_role],
            "WORKER_PRINCIPAL_OR_PHASE_INVALID")
    _positive(process["pid"], "WORKER_PROCESS_INVALID")
    validate_rfc3339_utc(process["started_at_utc"], field="started_at_utc")
    require(type(observation["sessions"]) is list
            and all(type(s) is dict and set(s) == {"principal", "worker_pid"}
                    and s["principal"] in {profile["principals"][worker_role],
                                           profile["principals"]["observer"]}
                    and type(s["worker_pid"]) is int and s["worker_pid"] > 0
                    and (s["principal"] != profile["principals"][worker_role]
                         or s["worker_pid"] == process["pid"])
                    for s in observation["sessions"]), "STALE_OR_UNATTRIBUTED_SESSION")
    required = {"host_backend", "host_front", "unrelated_container_backend",
                "unrelated_container_front", "worker_etcd", "worker_minio",
                "qualifier_mutation", "observer_mutation"}
    if phase != "construction":
        required |= {"revoked_writer_mutation", "revoked_writer_session"}
    require(type(observation["denials"]) is dict and set(observation["denials"]) == required,
            "CONTROL_DENIAL_PROOF_INCOMPLETE")
    for name, probe in observation["denials"].items():
        _shape(probe, {"request_sha256", "endpoint", "principal", "result"},
               "CONTROL_DENIAL_PROOF_INVALID")
        require(type(probe["request_sha256"]) is str and _HEX.fullmatch(probe["request_sha256"]) is not None
                and probe["result"] in {"NETWORK_DENIED", "AUTHORIZATION_DENIED"},
                "CONTROL_DENIAL_NOT_ESTABLISHED")
        _text(probe["endpoint"])
        _text(probe["principal"])
    return observation


def _go_join(*parts: str) -> bytes:
    # Go path.Join cleans a slash-joined string; Python join discards earlier
    # components when a later component is absolute.
    return posixpath.normpath(re.sub("/+", "/", "/".join(parts))).encode("utf-8")


def prefix_end(prefix: bytes) -> bytes:
    require(type(prefix) is bytes and bool(prefix) and b"\0" not in prefix, "ETCD_PREFIX_INVALID")
    result = bytearray(prefix)
    for i in range(len(result) - 1, -1, -1):
        if result[i] != 255:
            result[i] += 1
            return bytes(result[:i + 1])
    return b"\0"


def protected_ranges(etcd: Mapping[str, str]) -> tuple[tuple[bytes, bytes], ...]:
    prefixes = [_go_join(etcd["rootPath"], etcd["metaSubPath"]),
                _go_join(etcd["rootPath"], etcd["kvSubPath"]),
                _go_join(etcd["rootPath"], "config")]
    ranges = sorted((p, prefix_end(p)) for p in prefixes)
    merged: list[tuple[bytes, bytes]] = []
    for start, end in ranges:
        if merged and (merged[-1][1] == b"\0" or start <= merged[-1][1]):
            old_start, old_end = merged.pop()
            end = b"\0" if b"\0" in (old_end, end) else max(old_end, end)
            merged.append((old_start, end))
        else:
            merged.append((start, end))
    return tuple(merged)


def _latch_failure(method):
    @wraps(method)
    def guarded(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except BaseException as exc:
            self.fault(getattr(exc, "code", "CONTROL_INPUT_OR_TRANSPORT_FAILED"))
    return guarded


class WatchCoverage:
    """One snapshot plus live watches. Faults latch; no reconnect/resume method."""

    def __init__(self, *, etcd: Mapping[str, str], revision: int,
                 record: Callable[[str, Mapping[str, Any]], str],
                 invalidate: Callable[[str], None], clock_ns: Callable[[], int] = monotonic_ns) -> None:
        self.ranges = protected_ranges(etcd)
        self.q = _go_join(etcd["rootPath"], "config")
        self.revision = _positive(revision, "ETCD_REVISION_INVALID")
        self.start_revision = revision + 1
        self._record, self._invalidate, self._clock = record, invalidate, clock_ns
        self._fault: str | None = None
        self._pages: dict[int, list[dict[str, Any]]] = {i: [] for i in range(len(self.ranges))}
        self._next = {i: start for i, (start, _) in enumerate(self.ranges)}
        self._complete: set[int] = set()
        self._watermarks = {i: revision for i in range(len(self.ranges))}
        self._progress = {i: self._clock() for i in range(len(self.ranges))}
        self._keys = self._bytes = self._journal_bytes = 0
        self._watches: set[int] = set()

    def fault(self, code: str) -> None:
        if self._fault is None:
            self._fault = code
            self._invalidate(code)
        raise RematerializationError(self._fault)

    def check(self) -> None:
        if self._fault is not None:
            raise RematerializationError(self._fault)
        now = self._clock()
        if any(now < last or now - last > LIMITS["progress_deadline_ns"]
               for last in self._progress.values()):
            self.fault("WATCH_PROGRESS_DEADLINE")

    def _persist(self, kind: str, value: dict[str, Any]) -> None:
        size = len(strict_canonical_json_bytes(value))
        if self._journal_bytes + size > LIMITS["journal_bytes"]:
            self.fault("WATCH_JOURNAL_OVERFLOW")
        try:
            self._record(kind, value)
        except BaseException:
            self._fault = "WATCH_JOURNAL_DURABILITY_FAILED"
            self._invalidate(self._fault)
            raise
        self._journal_bytes += size

    @_latch_failure
    def snapshot_page(self, index: int, page: dict[str, Any]) -> None:
        try:
            self.check()
            _shape(page, {"request_revision", "header_revision", "start", "end", "more", "kvs"},
                   "SNAPSHOT_PAGE_INVALID")
            require(type(index) is int and index in self._next and index not in self._complete
                    and page["request_revision"] == self.revision
                    and type(page["header_revision"]) is int
                    and page["header_revision"] >= self.revision
                    and page["start"] == self._next[index] and page["end"] == self.ranges[index][1]
                    and type(page["more"]) is bool and type(page["kvs"]) is list,
                    "SNAPSHOT_PAGE_OR_REVISION_INVALID")
            start, end = self.ranges[index]
            previous = None
            for kv in page["kvs"]:
                _shape(kv, {"key", "value", "create_revision", "mod_revision", "lease"},
                       "SNAPSHOT_KV_INVALID")
                key, value = kv["key"], kv["value"]
                require(type(key) is bytes and type(value) is bytes
                        and key >= page["start"] and (end == b"\0" or key < end)
                        and (previous is None or key > previous)
                        and type(kv["mod_revision"]) is int
                        and type(kv["create_revision"]) is int
                        and 0 < kv["create_revision"] <= kv["mod_revision"] <= self.revision
                        and type(kv["lease"]) is int and kv["lease"] >= 0,
                        "SNAPSHOT_KV_INVALID")
                previous = key
                self._keys += 1
                self._bytes += len(key) + len(value)
                require(self._keys <= LIMITS["snapshot_keys"] and self._bytes <= LIMITS["snapshot_bytes"],
                        "SNAPSHOT_OVERFLOW")
                self._pages[index].append(dict(kv))
            require(not page["more"] or previous is not None, "SNAPSHOT_PAGINATION_INVALID")
            self._persist("CONTROL_SNAPSHOT_PAGE", {
                "range": index, "revision": self.revision, "start": page["start"].hex(),
                "end": end.hex(), "more": page["more"],
                "kvs": [{"key": kv["key"].hex(), "value_sha256": hashlib.sha256(kv["value"]).hexdigest(),
                         "create_revision": kv["create_revision"], "mod_revision": kv["mod_revision"],
                         "lease": kv["lease"]} for kv in page["kvs"]],
            })
            if page["more"]:
                self._next[index] = previous + b"\0"
            else:
                self._complete.add(index)
        except RematerializationError as exc:
            self.fault(exc.code)

    @property
    @_latch_failure
    def snapshot(self) -> tuple[dict[str, Any], ...]:
        self.check()
        require(len(self._complete) == len(self.ranges), "SNAPSHOT_INCOMPLETE")
        return tuple(dict(kv) for i in range(len(self.ranges)) for kv in self._pages[i])

    @_latch_failure
    def watch_created(self, index: int, *, start_revision: int, prev_kv: bool) -> None:
        self.check()
        require(type(index) is int and index in self._next and index not in self._watches
                and index in self._complete and type(start_revision) is int
                and start_revision == self.start_revision
                and prev_kv is True, "WATCH_START_OR_RESUME_INVALID")
        self._persist("WATCH_CREATED", {"range": index, "start_revision": start_revision,
                                        "prev_kv": True})
        self._watches.add(index)

    @_latch_failure
    def progress(self, index: int, *, revision: int, events_pending: int = 0) -> None:
        """Only a decoded native progress notification after queue processing."""
        self.check()
        require(type(index) is int and index in self._watches and type(revision) is int
                and revision >= self._watermarks[index]
                and type(events_pending) is int and events_pending == 0,
                "WATCH_PROGRESS_INVALID")
        self._persist("WATCH_PROCESSED_PROGRESS", {"range": index, "revision": revision})
        self._watermarks[index] = revision
        self._progress[index] = self._clock()

    @_latch_failure
    def events(self, index: int, *, events: list[dict[str, Any]], fragmented: bool = False) -> None:
        """Read-only interval: even attributable target catalog changes refuse.

        This intentionally conservative policy never grants harmlessness from
        a watch header or an unknown event. Native liveness needs integration.
        """
        self.check()
        require(type(index) is int and index in self._watches
                and fragmented is False and type(events) is list,
                "WATCH_REVISION_INCOMPLETE")
        if len(events) > LIMITS["pending_events"]:
            self.fault("WATCH_PENDING_OVERFLOW")
        size = 0
        for event in events:
            _shape(event, {"type", "revision", "key", "value", "previous"}, "WATCH_EVENT_INVALID")
            require(event["type"] in {"PUT", "DELETE"} and type(event["revision"]) is int
                    and event["revision"] > self.revision and type(event["key"]) is bytes
                    and type(event["value"]) is bytes and type(event["previous"]) is bytes,
                    "WATCH_EVENT_OR_PREVIOUS_INVALID")
            size += len(event["key"]) + len(event["value"]) + len(event["previous"])
        if size > LIMITS["pending_bytes"]:
            self.fault("WATCH_PENDING_OVERFLOW")
        for event in events:
            start, end = self.ranges[index]
            require(event["key"] >= start and (end == b"\0" or event["key"] < end),
                    "WATCH_EVENT_OUTSIDE_RANGE")
            self._persist("CONTROL_FORBIDDEN_EVENT", {
                "range": index, "type": event["type"], "revision": event["revision"],
                "key": event["key"].hex(),
                "value_sha256": hashlib.sha256(event["value"]).hexdigest(),
                "previous_sha256": hashlib.sha256(event["previous"]).hexdigest(),
            })
            self.fault("DYNAMIC_CONFIG_CHANGED" if event["key"].startswith(self.q)
                       else "PROTECTED_CATALOG_CHANGED")

    @_latch_failure
    def barrier(self, revision: int) -> dict[str, Any]:
        self.check()
        require(type(revision) is int and revision >= self.revision
                and len(self._watches) == len(self.ranges)
                and all(r >= revision for r in self._watermarks.values()),
                "WATCH_BARRIER_INCOMPLETE")
        return {"revision": revision,
                "processed_watermarks": [{"range": i, "revision": r}
                                         for i, r in sorted(self._watermarks.items())]}
