import { useState } from "react";
import {
  Download,
  KeyRound,
  PauseCircle,
  PlugZap,
  PlayCircle,
  Undo2,
  X,
  type LucideIcon,
} from "lucide-react";
import { useResource } from "@/app/scenario";
import { PageHeader } from "@/components/AppShell";
import { formatDateTime, GatePill, Pill } from "@/components/Indicators";
import { KeyValue, Mono, Panel } from "@/components/Panel";
import { Resource } from "@/components/StateViews";
import {
  BACKEND_NOT_CONNECTED_LABEL,
  COMMAND_ENDPOINT_BY_KIND,
  getCanary,
  getCommands,
  getSafetyGates,
  getStatus,
} from "@/lib/api";
import { demoCommandPreview } from "@/lib/demo-data";
import type { CommandKind, CommandState } from "@/lib/types";

const CARDS: { kind: CommandKind; label: string; description: string; icon: LucideIcon }[] = [
  {
    kind: "VERIFY_GRANT",
    label: "Verify operator grant",
    description: "Attest that the caller holds a valid, unexpired control grant.",
    icon: KeyRound,
  },
  {
    kind: "START_CANARY",
    label: "Start canary",
    description: "Begin routing a share of traffic to the reported candidate configuration.",
    icon: PlayCircle,
  },
  {
    kind: "PAUSE_ROUTING",
    label: "Pause adaptive routing",
    description: "Freeze allocation changes while keeping the current split in place.",
    icon: PauseCircle,
  },
  {
    kind: "REQUEST_ROLLBACK",
    label: "Request rollback",
    description: "Return all traffic to the reported last-known-good configuration.",
    icon: Undo2,
  },
  {
    kind: "EXPORT_AUDIT",
    label: "Export audit bundle",
    description: "Produce a signed evidence bundle for the selected experiment window.",
    icon: Download,
  },
];

const COMMAND_STATES: CommandState[] = ["QUEUED", "ACCEPTED", "REJECTED", "COMPLETED", "FAILED"];

const stateTone = (s: CommandState) =>
  s === "COMPLETED"
    ? "ok"
    : s === "ACCEPTED"
      ? "info"
      : s === "QUEUED"
        ? "neutral"
        : s === "REJECTED"
          ? "warn"
          : "danger";

export function CommandCenterPage() {
  const commands = useResource(getCommands, () => false);
  const gates = useResource(getSafetyGates, (d) => d.gates.length === 0);
  const status = useResource(getStatus, (d) => d.kpis.length === 0 && d.mode === "UNKNOWN");
  const canary = useResource(
    getCanary,
    (d) => d.lifecycle.length === 0 && !d.candidate && !d.split,
  );
  const [openKind, setOpenKind] = useState<CommandKind | null>(null);
  const [phrase, setPhrase] = useState("");
  const [reason, setReason] = useState("");

  const configIdentity =
    canary.state.kind === "ready"
      ? (canary.state.data.candidate?.config ?? canary.state.data.lkg?.config)
      : status.state.kind === "ready"
        ? status.state.data.config
        : undefined;
  const datasetIdentity = status.state.kind === "ready" ? status.state.data.dataset : undefined;
  const openCard = CARDS.find((c) => c.kind === openKind);

  const close = () => {
    setOpenKind(null);
    setPhrase("");
    setReason("");
  };

  return (
    <>
      <PageHeader
        title="Command Center"
        description="Control actions are surfaced with their exact target, identities and confirmation requirements. In this build no request is sent, nothing is queued, and no state changes."
        aside={
          <Pill tone="warn" icon={<PlugZap className="h-3.5 w-3.5" aria-hidden />}>
            {BACKEND_NOT_CONNECTED_LABEL}
          </Pill>
        }
      />

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {CARDS.map(({ kind, label, description, icon: Icon }) => (
          <div key={kind} className="panel flex flex-col gap-3 px-4 py-4">
            <div className="flex items-start gap-3">
              <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md border border-border bg-surface-2 text-telemetry">
                <Icon className="h-4 w-4" aria-hidden />
              </span>
              <div className="min-w-0">
                <h3 className="text-sm font-semibold text-foreground">{label}</h3>
                <p className="mt-1 text-xs text-muted-foreground">{description}</p>
              </div>
            </div>
            <Mono className="block truncate">POST {COMMAND_ENDPOINT_BY_KIND[kind]}</Mono>
            <div className="mt-auto flex flex-wrap items-center gap-2">
              <button type="button" className="btn-ghost" onClick={() => setOpenKind(kind)}>
                Preview confirmation
              </button>
              <button
                type="button"
                disabled
                aria-disabled="true"
                title={BACKEND_NOT_CONNECTED_LABEL}
                className="btn-ghost"
              >
                {BACKEND_NOT_CONNECTED_LABEL}
              </button>
            </div>
          </div>
        ))}
      </div>

      <div className="grid gap-6 xl:grid-cols-[1.2fr_1fr]">
        <Panel
          title="Command history"
          subtitle="Queued, accepted, rejected, completed and failed submissions appear here once a control service is connected."
        >
          <Resource
            state={commands.state}
            onRetry={commands.reload}
            loadingLabel="Reading command history"
          >
            {(data) => (
              <div className="space-y-4">
                <div className="flex flex-wrap gap-1.5">
                  {COMMAND_STATES.map((s) => (
                    <Pill key={s} tone={stateTone(s)}>
                      {s} · 0
                    </Pill>
                  ))}
                </div>
                {data.history.length === 0 ? (
                  <div className="rounded-lg border border-dashed border-border px-4 py-8 text-center">
                    <p className="text-sm font-medium text-foreground">No commands recorded</p>
                    <p className="mx-auto mt-1 max-w-md text-xs text-muted-foreground">
                      Phase A never submits a command, so history is empty by construction.
                      Submission is disabled: {data.disabledReason ?? BACKEND_NOT_CONNECTED_LABEL}.
                    </p>
                  </div>
                ) : (
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Command</th>
                        <th>State</th>
                        <th>Submitted</th>
                        <th>Target</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.history.map((c) => (
                        <tr key={c.id}>
                          <td className="font-mono text-xs">{c.kind}</td>
                          <td>
                            <Pill tone={stateTone(c.state)}>{c.state}</Pill>
                          </td>
                          <td className="font-mono text-xs">{formatDateTime(c.submittedAt)}</td>
                          <td className="font-mono text-xs">{c.target ?? "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            )}
          </Resource>
        </Panel>

        <Panel
          title="Safety gates"
          subtitle="Required gates must pass before any submission can be enabled."
        >
          <Resource state={gates.state} onRetry={gates.reload} loadingLabel="Reading safety gates">
            {(data) => (
              <div className="space-y-4">
                <div className="flex items-center gap-2">
                  <span className="label-mono">Overall</span>
                  <GatePill state={data.overall} />
                  {data.policyRevision && <Mono className="ml-auto">{data.policyRevision}</Mono>}
                </div>
                <ul className="space-y-2.5">
                  {data.gates.map((gate) => (
                    <li key={gate.id} className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="text-sm text-foreground">
                          {gate.label}
                          {gate.required && (
                            <span className="ml-1.5 font-mono text-[10px] text-muted-foreground">
                              REQUIRED
                            </span>
                          )}
                        </p>
                        {gate.detail && <Mono className="block">{gate.detail}</Mono>}
                      </div>
                      <GatePill state={gate.state} />
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </Resource>
        </Panel>
      </div>

      {openCard && (
        <div
          role="dialog"
          aria-modal="true"
          aria-labelledby="cmd-dialog-title"
          className="fixed inset-0 z-50 flex items-center justify-center bg-background/80 p-4 backdrop-blur-sm"
        >
          <div className="panel max-h-[90vh] w-full max-w-2xl overflow-y-auto">
            <header className="flex items-start justify-between gap-4 border-b border-border/70 px-5 py-4">
              <div>
                <h3 id="cmd-dialog-title" className="text-sm font-semibold text-foreground">
                  Confirm: {openCard.label}
                </h3>
                <Mono className="mt-0.5 block">POST {COMMAND_ENDPOINT_BY_KIND[openCard.kind]}</Mono>
              </div>
              <button
                type="button"
                onClick={close}
                className="btn-ghost"
                aria-label="Close preview"
              >
                <X className="h-3.5 w-3.5" aria-hidden />
              </button>
            </header>

            <div className="space-y-5 px-5 py-4">
              <div className="rounded-lg border border-warn/40 bg-warn/5 px-4 py-3 text-xs text-warn">
                {demoCommandPreview.warning}
              </div>

              <div className="grid gap-4 sm:grid-cols-2">
                <KeyValue label="Exact target" value={<Mono>{openCard.kind}</Mono>} />
                <KeyValue
                  label="Config identity"
                  value={
                    <Mono>
                      {configIdentity
                        ? `${configIdentity.label ?? configIdentity.id} (${configIdentity.id})`
                        : "Not reported"}
                    </Mono>
                  }
                />
                <KeyValue label="Config hash" value={<Mono>{configIdentity?.hash ?? "—"}</Mono>} />
                <KeyValue
                  label="Dataset identity"
                  value={
                    <Mono>
                      {datasetIdentity
                        ? `${datasetIdentity.label ?? datasetIdentity.id} (${datasetIdentity.id})`
                        : "Not reported"}
                    </Mono>
                  }
                />
                <KeyValue
                  label="Dataset hash"
                  value={<Mono>{datasetIdentity?.hash ?? "—"}</Mono>}
                />
                <KeyValue
                  label="Submission"
                  value={<Pill tone="warn">{BACKEND_NOT_CONNECTED_LABEL}</Pill>}
                />
              </div>

              <div>
                <label htmlFor="cmd-reason" className="label-mono">
                  Reason
                </label>
                <textarea
                  id="cmd-reason"
                  rows={2}
                  value={reason}
                  onChange={(e) => setReason(e.target.value)}
                  placeholder={demoCommandPreview.reasonPlaceholder}
                  className="mt-1.5 w-full rounded-md border border-border bg-surface-2 px-3 py-2 text-sm text-foreground outline-none focus-visible:border-telemetry focus-visible:ring-2 focus-visible:ring-telemetry/30"
                />
              </div>

              <div>
                <label htmlFor="cmd-phrase" className="label-mono">
                  Type “{demoCommandPreview.requiredPhrase}” to arm
                </label>
                <input
                  id="cmd-phrase"
                  value={phrase}
                  onChange={(e) => setPhrase(e.target.value)}
                  aria-describedby="cmd-phrase-hint"
                  className="mt-1.5 w-full rounded-md border border-border bg-surface-2 px-3 py-2 font-mono text-sm text-foreground outline-none focus-visible:border-telemetry focus-visible:ring-2 focus-visible:ring-telemetry/30"
                />
                <p id="cmd-phrase-hint" className="mt-1.5 text-xs text-muted-foreground">
                  {phrase === demoCommandPreview.requiredPhrase
                    ? "Phrase matched. Submission remains disabled: backend not connected."
                    : "Phrase not matched."}
                </p>
              </div>
            </div>

            <footer className="flex flex-wrap items-center justify-end gap-2 border-t border-border/70 px-5 py-4">
              <button type="button" onClick={close} className="btn-ghost">
                Cancel
              </button>
              <button
                type="button"
                disabled
                aria-disabled="true"
                title={BACKEND_NOT_CONNECTED_LABEL}
                className="btn-ghost"
              >
                {BACKEND_NOT_CONNECTED_LABEL}
              </button>
            </footer>
          </div>
        </div>
      )}
    </>
  );
}
