import assert from "node:assert/strict";
import test from "node:test";
import {
  createStreamAdapter,
  getStatus,
  postExportAudit,
  postPauseRouting,
  postRequestRollback,
  postStartCanary,
  postVerifyGrant,
} from "./api";
import { BackendNotConnectedError, type CommandRequest } from "./types";

const commandRequest: CommandRequest = {
  kind: "VERIFY_GRANT",
  reason: "Phase-A safety contract test",
  typedPhrase: "VERIFY",
};

test("Phase-A reads report demo data without reaching a backend", async () => {
  const response = await getStatus("normal");

  assert.equal(response.ok, true);
  assert.equal(response.demo, true);
  assert.equal(response.data?.mode, "DRY_RUN");
});

test("Phase-A keeps every command path disabled", async () => {
  const commands = [
    postVerifyGrant,
    postStartCanary,
    postPauseRouting,
    postRequestRollback,
    postExportAudit,
  ];

  for (const command of commands) {
    await assert.rejects(command(commandRequest), BackendNotConnectedError);
  }
});

test("Phase-A keeps the stream adapter disconnected and event-free", () => {
  const adapter = createStreamAdapter();
  let received = 0;
  const unsubscribe = adapter.subscribe(() => {
    received += 1;
  });

  assert.equal(adapter.state, "DISCONNECTED");
  unsubscribe();
  adapter.close();
  assert.equal(received, 0);
});
