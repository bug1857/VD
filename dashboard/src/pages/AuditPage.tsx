import { useMemo, useState } from "react";
import { Download, Search, ShieldCheck } from "lucide-react";
import { useResource } from "@/app/scenario";
import { PageHeader } from "@/components/AppShell";
import { EvidencePill, formatDateTime, Pill } from "@/components/Indicators";
import { KeyValue, Mono, Panel } from "@/components/Panel";
import { Resource } from "@/components/StateViews";
import { BACKEND_NOT_CONNECTED_LABEL, getAudit } from "@/lib/api";
import { EVIDENCE_STATUSES, type EvidenceStatus } from "@/lib/types";

export function AuditPage() {
  const audit = useResource(getAudit, (d) => d.records.length === 0);
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState<EvidenceStatus | "ALL">("ALL");

  const filtered = useMemo(() => {
    const records = audit.state.kind === "ready" ? audit.state.data.records : [];
    const q = query.trim().toLowerCase();
    return records.filter((r) => {
      if (status !== "ALL" && r.evidenceStatus !== status) return false;
      if (!q) return true;
      return [
        r.id,
        r.actor,
        r.action,
        r.target,
        r.configHash,
        r.datasetHash,
        r.payloadHash,
        r.experimentId,
        r.sourceRevision,
        r.note,
      ]
        .filter(Boolean)
        .some((v) => String(v).toLowerCase().includes(q));
    });
  }, [audit.state, query, status]);

  return (
    <>
      <PageHeader
        title="Audit & Evidence"
        description="Immutable record of evaluated windows, gate decisions and promotions, with content hashes, verification method and the source revision that produced each entry."
        aside={
          <button
            type="button"
            disabled
            aria-disabled="true"
            title={`${BACKEND_NOT_CONNECTED_LABEL} — export is produced server-side`}
            className="btn-ghost"
          >
            <Download className="h-3.5 w-3.5" aria-hidden /> Export bundle ·{" "}
            {BACKEND_NOT_CONNECTED_LABEL}
          </button>
        }
      />

      <Resource
        state={audit.state}
        onRetry={audit.reload}
        loadingLabel="Reading audit records"
        empty={{ title: "No audit records", detail: "No entries exist for the selected window." }}
      >
        {(data) => (
          <div className="space-y-6">
            <Panel
              title="Audit records"
              subtitle={`${filtered.length} of ${data.records.length} entries · source revision ${data.sourceRevision ?? "not reported"}`}
              actions={
                <div className="flex flex-wrap items-center gap-2">
                  <div className="relative">
                    <Search
                      className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground"
                      aria-hidden
                    />
                    <input
                      value={query}
                      onChange={(e) => setQuery(e.target.value)}
                      placeholder="Search actor, action, hash…"
                      aria-label="Search audit records"
                      className="w-56 rounded-md border border-border bg-surface-2 py-1.5 pl-8 pr-3 font-mono text-xs text-foreground outline-none focus-visible:border-telemetry focus-visible:ring-2 focus-visible:ring-telemetry/30"
                    />
                  </div>
                  <select
                    value={status}
                    onChange={(e) => setStatus(e.target.value as EvidenceStatus | "ALL")}
                    aria-label="Filter by evidence status"
                    className="rounded-md border border-border bg-surface-2 px-2.5 py-1.5 font-mono text-xs text-foreground outline-none focus-visible:border-telemetry"
                  >
                    <option value="ALL">All evidence statuses</option>
                    {EVIDENCE_STATUSES.map((s) => (
                      <option key={s} value={s}>
                        {s}
                      </option>
                    ))}
                  </select>
                </div>
              }
            >
              {filtered.length === 0 ? (
                <div className="rounded-lg border border-dashed border-border px-4 py-8 text-center">
                  <p className="text-sm font-medium text-foreground">
                    No records match this filter
                  </p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    Clear the search or evidence status filter.
                  </p>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Recorded</th>
                        <th>Actor</th>
                        <th>Action</th>
                        <th>Target</th>
                        <th>Evidence</th>
                        <th>Hashes</th>
                        <th>Verification</th>
                        <th>Experiment</th>
                        <th>Revision</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filtered.map((r) => (
                        <tr key={r.id}>
                          <td className="font-mono text-xs whitespace-nowrap">
                            {formatDateTime(r.at)}
                          </td>
                          <td className="font-mono text-xs">{r.actor}</td>
                          <td className="text-xs text-foreground">
                            {r.action}
                            {r.note && (
                              <Mono className="mt-0.5 block max-w-xs whitespace-normal">
                                {r.note}
                              </Mono>
                            )}
                          </td>
                          <td className="font-mono text-xs">{r.target ?? "—"}</td>
                          <td>
                            <EvidencePill status={r.evidenceStatus} />
                          </td>
                          <td className="font-mono text-[11px] leading-relaxed text-muted-foreground">
                            <div>cfg {r.configHash ?? "—"}</div>
                            <div>ds {r.datasetHash ?? "—"}</div>
                            <div>payload {r.payloadHash ?? "—"}</div>
                          </td>
                          <td className="font-mono text-[11px] text-muted-foreground">
                            {r.verification ? (
                              <>
                                <div className="flex items-center gap-1 text-ok">
                                  <ShieldCheck className="h-3 w-3" aria-hidden />{" "}
                                  {r.verification.method ?? "verified"}
                                </div>
                                <div>{formatDateTime(r.verification.verifiedAt)}</div>
                                {r.verification.signature && <div>{r.verification.signature}</div>}
                              </>
                            ) : (
                              "not verified"
                            )}
                          </td>
                          <td className="font-mono text-xs">{r.experimentId ?? "—"}</td>
                          <td className="font-mono text-xs">{r.sourceRevision ?? "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Panel>

            <div className="grid gap-6 xl:grid-cols-[1fr_1fr]">
              <Panel title="Experiments" subtitle="Grouping key for evidence and replay bundles.">
                {data.experiments?.length ? (
                  <ul className="space-y-2.5">
                    {data.experiments.map((exp) => (
                      <li
                        key={exp.id}
                        className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border/70 bg-surface-2/40 px-3 py-2.5"
                      >
                        <div className="min-w-0">
                          <p className="truncate text-sm text-foreground">{exp.label}</p>
                          <Mono className="block">
                            {exp.id} · {exp.sourceRevision ?? "revision not reported"}
                          </Mono>
                        </div>
                        <Pill
                          tone={
                            exp.state === "RUNNING"
                              ? "info"
                              : exp.state === "COMPLETED"
                                ? "ok"
                                : "muted"
                          }
                        >
                          {exp.state}
                        </Pill>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="text-sm text-muted-foreground">No experiments reported.</p>
                )}
              </Panel>

              <Panel
                title="Export"
                subtitle="Signed bundles are produced by the control service only."
              >
                <div className="space-y-4">
                  <div className="grid gap-4 sm:grid-cols-2">
                    <KeyValue
                      label="Export enabled"
                      value={<Mono>{String(data.exportEnabled)}</Mono>}
                    />
                    <KeyValue label="Reason" value={<Mono>{BACKEND_NOT_CONNECTED_LABEL}</Mono>} />
                  </div>
                  <button
                    type="button"
                    disabled
                    aria-disabled="true"
                    className="btn-ghost"
                    title={BACKEND_NOT_CONNECTED_LABEL}
                  >
                    <Download className="h-3.5 w-3.5" aria-hidden /> {BACKEND_NOT_CONNECTED_LABEL}
                  </button>
                  <p className="text-xs text-muted-foreground">
                    Client-side export is intentionally absent: an evidence bundle is only
                    trustworthy when the service signs it. Nothing here is downloaded or persisted.
                  </p>
                </div>
              </Panel>
            </div>
          </div>
        )}
      </Resource>
    </>
  );
}
