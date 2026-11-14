# dapr.ext.workflow

Durable workflow orchestration for the Dapr Python SDK, built on a vendored
durabletask engine. Provides `WorkflowRuntime` and `DaprWorkflowClient`.

```sh
pip install "dapr[workflow]"
```

```python
from dapr.ext.workflow import WorkflowRuntime, DaprWorkflowClient
```

See the root [README](../../../README.md) for migration steps from the legacy
`dapr-ext-workflow` distribution.

## Workflow payload references (Alpha)

Workflow and activity inputs and outputs are recorded in workflow history. Keep these values
small: pass status, identifiers, and other metadata through the workflow, and place large data in
a Dapr Binary Store. `PayloadReference` is a JSON-compatible envelope containing the component
name, object key, payload size, and SHA-256 digest. Its wire format is shared with the .NET and
JavaScript workflow SDKs.

The resolver is explicit; it does not change the serialization or execution behavior of existing
workflows. Use it from an application client or an activity, never from deterministic orchestrator
code. For example, a workflow can return a reference from one activity and pass it to another:

```python
from dapr.clients import DaprClient
from dapr.ext.workflow import WorkflowPayloadResolver

# Keep the Dapr client open for as long as the worker can execute these activities.
dapr = DaprClient()
payloads = WorkflowPayloadResolver(dapr, store_name='workflowdata')


def save_large_result(ctx, value):
    return payloads.store(value, ctx.workflow_id)


def process_large_result(ctx, reference):
    value = payloads.resolve(reference)
    return process(value)


def my_workflow(ctx, value):
    reference = yield ctx.call_activity(save_large_result, input=value)
    return (yield ctx.call_activity(process_large_result, input=reference))

# Close the Dapr client after the workflow worker has shut down.
```

`PayloadReference` is a `dict` subclass, so it can be returned from an activity and passed directly
as the next activity's input. The consuming activity calls `resolve()` to retrieve and deserialize
the original value. The same resolver also exposes `store_bytes()` / `resolve_bytes()` for raw
bytes, and `store_stream()` / `open()` for binary content that should remain streamed:

```python
with open('movie.mp4', 'rb') as source:
    reference = payloads.store_stream(source, instance_id='workflow-123')

with payloads.open(reference) as content:
    for chunk in content:
        process_chunk(chunk)
```

For async applications and activities, use `AsyncWorkflowPayloadResolver` from
`dapr.ext.workflow.aio` with `dapr.aio.clients.DaprClient`. It exposes the same typed and raw-byte
operations; `store_stream()` accepts an `AsyncIterable[bytes]`, and `open()` returns an async
stream.

Buffered `resolve()` and `resolve_bytes()` calls are limited to 16 MiB by default. Configure
`max_rehydration_size` to change that limit, or use `open()` for streaming reads. Streamed reads do
not buffer the entire object and verify the recorded digest when the stream is consumed to EOF;
closing a stream early cancels the download before the final integrity check. The resolver never
deletes payloads automatically when a workflow is purged. Retain or delete
referenced objects according to an application-owned retention policy, and only call `delete()`
when no live or replayable workflow still needs the object.

The Binary Store API and workflow payload references are alpha and subject to change. Configure the
named Binary Store component in the Dapr application, and ensure every application that consumes a
reference has access to that component and object. Payload references reduce the data stored in
workflow history; they do not raise the sidecar dispatch limit or change the actor state store used
for workflow history.