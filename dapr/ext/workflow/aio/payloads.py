# -*- coding: utf-8 -*-

"""
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from __future__ import annotations

import asyncio
import hashlib
import tempfile
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

from dapr.ext.workflow.payloads import (
    PayloadReference,
    PayloadRehydrationLimitError,
    _SPOOL_MEMORY_LIMIT,
    _WorkflowPayloadResolverBase,
)


class AsyncWorkflowPayloadResolver(_WorkflowPayloadResolverBase):
    """Async resolver for Dapr Binary Store payloads.

    Use from async activities or application code. Deterministic orchestrator code must not do I/O.
    """

    async def store(self, value: Any, instance_id: str) -> PayloadReference[Any]:
        """Serialize and store a value, returning its JSON-compatible reference."""
        reference = await self.store_bytes(self._serialize_value(value), instance_id)
        return cast(PayloadReference[Any], reference)

    async def store_bytes(self, content: bytes, instance_id: str) -> PayloadReference[bytes]:
        """Store raw bytes and return a reference."""
        if not isinstance(content, (bytes, bytearray, memoryview)):
            raise TypeError('content must be bytes-like')
        payload = bytes(content)
        digest = hashlib.sha256(payload).hexdigest()
        reference = self._new_reference(instance_id, len(payload), digest)
        await self._client.set_binary_file_alpha1(
            reference.store_name, reference.key, payload, overwrite=True
        )
        return cast(PayloadReference[bytes], reference)

    async def store_stream(
        self, chunks: AsyncIterable[bytes], instance_id: str
    ) -> PayloadReference[bytes]:
        """Store an async binary stream without buffering the complete payload in memory."""
        digest = hashlib.sha256()
        size = 0
        with tempfile.SpooledTemporaryFile(max_size=_SPOOL_MEMORY_LIMIT, mode='w+b') as staged:
            async for chunk in chunks:
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    raise TypeError('each content chunk must be bytes-like')
                payload = bytes(chunk)
                digest.update(payload)
                size += len(payload)
                await asyncio.to_thread(staged.write, payload)
            reference = self._new_reference(instance_id, size, digest.hexdigest())
            await asyncio.to_thread(staged.seek, 0)
            await self._client.set_binary_file_alpha1(
                reference.store_name, reference.key, staged, overwrite=True
            )
        return cast(PayloadReference[bytes], reference)

    async def resolve_bytes(self, reference: object) -> bytes:
        """Download and integrity-check bytes, enforcing the configured buffer limit."""
        parsed = self._coerce_reference(reference)
        self._ensure_supported_stages(parsed)
        self._check_rehydration_size(parsed)
        chunks: list[bytes] = []
        size = 0
        async with self.open(parsed) as response:
            async for chunk in response:
                size += len(chunk)
                limit = self.max_rehydration_size
                if limit is not None and size > limit:
                    raise PayloadRehydrationLimitError(parsed, limit)
                chunks.append(chunk)
        return b''.join(chunks)

    async def resolve(self, reference: object) -> Any:
        """Download, integrity-check, and deserialize a stored workflow value."""
        return self._deserialize_value(await self.resolve_bytes(reference))

    @asynccontextmanager
    async def open(self, reference: object) -> AsyncIterator[AsyncIterator[bytes]]:
        """Open raw bytes as a verified async stream; integrity is checked at EOF."""
        parsed = self._coerce_reference(reference)
        self._ensure_supported_stages(parsed)
        digest = hashlib.sha256()
        size = 0
        response = await self._client.get_binary_file_alpha1(parsed.store_name, parsed.key)
        async with response:
            async def verified_chunks() -> AsyncIterator[bytes]:
                nonlocal size
                async for chunk in response:
                    size += len(chunk)
                    digest.update(chunk)
                    yield chunk
                self._verify_payload(parsed, size, digest.hexdigest())

            yield verified_chunks()

    async def delete(self, reference: object) -> None:
        """Delete one explicitly selected payload. Deletion is never automatic."""
        parsed = self._coerce_reference(reference)
        self._ensure_supported_stages(parsed)
        await self._client.delete_binary_file_alpha1(parsed.store_name, parsed.key)
