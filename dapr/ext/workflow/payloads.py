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

import hashlib
import json
import re
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any, BinaryIO, Generic, Optional, TypeVar, cast
from urllib.parse import quote

T = TypeVar('T')

_REFERENCE_PROPERTY = '$daprRef'
_REFERENCE_VERSION = 1
_MAX_SAFE_JSON_INTEGER = (1 << 53) - 1
_DEFAULT_KEY_PREFIX = 'workflow-payloads'
_DEFAULT_MAX_REHYDRATION_SIZE = 16 * 1024 * 1024
_SPOOL_MEMORY_LIMIT = 8 * 1024 * 1024
_IO_CHUNK_SIZE = 64 * 1024
_SHA256_PATTERN = re.compile(r'^[0-9a-f]{64}$')


class PayloadReference(dict[str, Any], Generic[T]):
    """JSON-compatible reference envelope for an object in a Dapr Binary Store.

    The single-property $daprRef envelope is shared with the .NET and JavaScript SDKs.
    """

    def __init__(
        self,
        store_name: str,
        key: str,
        size: int,
        content_hash: str,
        stages: Optional[list[Mapping[str, Any]]] = None,
        version: int = _REFERENCE_VERSION,
    ) -> None:
        if (
            not isinstance(version, int)
            or isinstance(version, bool)
            or version != _REFERENCE_VERSION
        ):
            raise ValueError(f'Unsupported workflow payload reference version: {version}')
        if not isinstance(store_name, str) or not store_name.strip():
            raise ValueError('store_name must not be empty')
        if not isinstance(key, str) or not key.strip():
            raise ValueError('key must not be empty')
        if (
            not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
            or size > _MAX_SAFE_JSON_INTEGER
        ):
            raise ValueError('size must be a non-negative safe JSON integer')
        if not isinstance(content_hash, str) or not _SHA256_PATTERN.fullmatch(content_hash):
            raise ValueError('content_hash must be a lowercase SHA-256 hex digest')

        normalized_stages: list[dict[str, Any]] = []
        for stage in stages or []:
            if not isinstance(stage, Mapping):
                raise ValueError('each payload stage must be an object')
            name = stage.get('name')
            metadata = stage.get('metadata')
            if not isinstance(name, str) or not name.strip():
                raise ValueError('each payload stage must have a non-empty name')
            if metadata is not None and (
                not isinstance(metadata, Mapping)
                or any(
                    not isinstance(k, str) or not isinstance(v, str)
                    for k, v in metadata.items()
                )
            ):
                raise ValueError('payload stage metadata must contain string keys and values')
            normalized_stages.append(
                {'n': name, **({'m': dict(metadata)} if metadata is not None else {})}
            )

        body: dict[str, Any] = {
            'v': version,
            'store': store_name,
            'key': key,
            'size': size,
            'sha256': content_hash,
        }
        if normalized_stages:
            body['stages'] = normalized_stages
        super().__init__({_REFERENCE_PROPERTY: body})

    @property
    def _body(self) -> Mapping[str, Any]:
        return self[_REFERENCE_PROPERTY]

    @property
    def store_name(self) -> str:
        return self._body['store']

    @property
    def key(self) -> str:
        return self._body['key']

    @property
    def size(self) -> int:
        return self._body['size']

    @property
    def content_hash(self) -> str:
        return self._body['sha256']

    @property
    def stages(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(self._body.get('stages', ()))

    @property
    def version(self) -> int:
        return self._body['v']

    def as_typed(self) -> PayloadReference[T]:
        """Return this reference with a more specific static type."""
        return self

    def to_envelope(self) -> str:
        """Serialize the reference to compact JSON."""
        return json.dumps(self, separators=(',', ':'), ensure_ascii=False)

    @classmethod
    def try_parse(cls, value: object) -> Optional[PayloadReference[Any]]:
        """Parse a reference, returning None for ordinary workflow values.

        An object that claims the reserved $daprRef property but is malformed raises ValueError.
        """
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError as error:
                if _REFERENCE_PROPERTY in value:
                    raise ValueError('Malformed workflow payload reference JSON') from error
                return None

        if not isinstance(value, Mapping) or _REFERENCE_PROPERTY not in value:
            return None
        if len(value) != 1:
            raise ValueError('A workflow payload reference must contain only $daprRef')

        body = value[_REFERENCE_PROPERTY]
        if not isinstance(body, Mapping):
            raise ValueError('Invalid workflow payload reference envelope')
        if set(body) - {'v', 'store', 'key', 'size', 'sha256', 'stages'}:
            raise ValueError('Unknown fields in workflow payload reference envelope')
        if body.get('v') != _REFERENCE_VERSION:
            raise ValueError(f'Unsupported workflow payload reference version: {body.get("v")}')

        stages_value = body.get('stages')
        stages: list[Mapping[str, Any]] = []
        if stages_value is not None:
            if not isinstance(stages_value, list):
                raise ValueError('Workflow payload reference stages must be an array')
            for stage in stages_value:
                if not isinstance(stage, Mapping) or set(stage) - {'n', 'm'}:
                    raise ValueError('Invalid workflow payload reference stage')
                metadata = stage.get('m')
                if metadata is not None and (
                    not isinstance(metadata, Mapping)
                    or any(
                        not isinstance(k, str) or not isinstance(v, str)
                        for k, v in metadata.items()
                    )
                ):
                    raise ValueError('Invalid workflow payload reference stage metadata')
                stages.append(
                    {
                        'name': stage.get('n'),
                        **({'metadata': dict(metadata)} if metadata is not None else {}),
                    }
                )

        return cls(
            store_name=body.get('store'),
            key=body.get('key'),
            size=body.get('size'),
            content_hash=body.get('sha256'),
            stages=stages,
            version=body.get('v'),
        )

    @classmethod
    def parse(cls, value: object) -> PayloadReference[Any]:
        """Parse a reference or raise ValueError when the value is not one."""
        reference = cls.try_parse(value)
        if reference is None:
            raise ValueError('Value is not a workflow payload reference')
        return reference


class PayloadIntegrityError(ValueError):
    """Raised when downloaded bytes do not match a payload reference."""

    def __init__(self, reference: PayloadReference[Any], size: int, content_hash: str) -> None:
        self.reference = reference
        self.actual_size = size
        self.actual_hash = content_hash
        super().__init__(
            f'Payload integrity check failed for {reference.store_name}/{reference.key}: '
            f'expected {reference.size} bytes with SHA-256 {reference.content_hash}, '
            f'got {size} bytes with SHA-256 {content_hash}'
        )


class PayloadRehydrationLimitError(ValueError):
    """Raised when a buffered read exceeds the configured in-memory limit."""

    def __init__(self, reference: PayloadReference[Any], limit: int) -> None:
        self.reference = reference
        self.limit = limit
        super().__init__(
            f'Payload is {reference.size} bytes, exceeding the in-memory limit of {limit}; '
            'use open() to consume it as a stream'
        )


class _WorkflowPayloadResolverBase:
    def __init__(
        self,
        client: Any,
        store_name: str,
        *,
        key_prefix: str = _DEFAULT_KEY_PREFIX,
        max_rehydration_size: Optional[int] = _DEFAULT_MAX_REHYDRATION_SIZE,
    ) -> None:
        methods = (
            'set_binary_file_alpha1',
            'get_binary_file_alpha1',
            'delete_binary_file_alpha1',
        )
        if any(not callable(getattr(client, method, None)) for method in methods):
            raise TypeError('client must provide the Dapr Binary Store alpha1 methods')
        if not isinstance(store_name, str) or not store_name.strip():
            raise ValueError('store_name must not be empty')
        self._validate_key_prefix(key_prefix)
        if max_rehydration_size is not None and (
            not isinstance(max_rehydration_size, int)
            or isinstance(max_rehydration_size, bool)
            or max_rehydration_size <= 0
        ):
            raise ValueError('max_rehydration_size must be positive or None')
        self._client = client
        self.store_name = store_name
        self.key_prefix = key_prefix
        self.max_rehydration_size = max_rehydration_size

    @staticmethod
    def _validate_key_prefix(prefix: str) -> None:
        if (
            not isinstance(prefix, str)
            or not prefix.strip()
            or prefix.startswith('/')
            or prefix.endswith('/')
            or any(part in {'', '.', '..'} for part in prefix.split('/'))
            or any(char in prefix for char in '\\:*?[]#%')
            or any(ord(char) < 32 for char in prefix)
        ):
            raise ValueError(
                'key_prefix must be a relative Binary Store path without reserved segments'
            )

    @staticmethod
    def _serialize_value(value: Any) -> bytes:
        from dapr.ext.workflow._durabletask.internal.shared import to_json

        return to_json(value).encode('utf-8')

    @staticmethod
    def _deserialize_value(content: bytes) -> Any:
        from dapr.ext.workflow._durabletask.internal.shared import from_json

        return from_json(content.decode('utf-8'))

    def _new_reference(self, instance_id: str, size: int, digest: str) -> PayloadReference[Any]:
        if not isinstance(instance_id, str) or not instance_id.strip():
            raise ValueError('instance_id must not be empty')
        segment = quote(instance_id, safe='-_.~')
        if segment in {'.', '..'}:
            segment = segment.replace('.', '%2E')
        return PayloadReference(
            store_name=self.store_name,
            key=f'{self.key_prefix}/{segment}/{digest}',
            size=size,
            content_hash=digest,
        )

    @staticmethod
    def _coerce_reference(reference: object) -> PayloadReference[Any]:
        return PayloadReference.parse(reference)

    @staticmethod
    def _ensure_supported_stages(reference: PayloadReference[Any]) -> None:
        if reference.stages:
            raise NotImplementedError(
                'This Python SDK version does not support payload stage transformations'
            )

    def _check_rehydration_size(self, reference: PayloadReference[Any]) -> None:
        limit = self.max_rehydration_size
        if limit is not None and reference.size > limit:
            raise PayloadRehydrationLimitError(reference, limit)

    @staticmethod
    def _verify_payload(reference: PayloadReference[Any], size: int, digest: str) -> None:
        if size != reference.size or digest != reference.content_hash:
            raise PayloadIntegrityError(reference, size, digest)


class WorkflowPayloadResolver(_WorkflowPayloadResolverBase):
    """Stores and resolves workflow payloads through an existing Dapr Binary Store.

    Use this class from application code or activities, never from deterministic orchestrator code.
    Returned references are JSON objects that can be passed through workflow inputs, activity
    inputs/results, child workflows, and external events.
    """

    def store(self, value: T, instance_id: str) -> PayloadReference[T]:
        """Serialize and store a value, returning its JSON-compatible reference."""
        return cast(PayloadReference[T], self.store_bytes(self._serialize_value(value), instance_id))

    def store_bytes(self, content: bytes, instance_id: str) -> PayloadReference[bytes]:
        """Store raw bytes and return a reference."""
        if not isinstance(content, (bytes, bytearray, memoryview)):
            raise TypeError('content must be bytes-like')
        payload = bytes(content)
        digest = hashlib.sha256(payload).hexdigest()
        reference = self._new_reference(instance_id, len(payload), digest)
        self._client.set_binary_file_alpha1(
            reference.store_name, reference.key, payload, overwrite=True
        )
        return cast(PayloadReference[bytes], reference)

    def store_stream(self, content: BinaryIO, instance_id: str) -> PayloadReference[bytes]:
        """Store a binary stream without buffering the complete payload in memory."""
        if not callable(getattr(content, 'read', None)):
            raise TypeError('content must be a readable binary stream')
        digest = hashlib.sha256()
        size = 0
        with tempfile.SpooledTemporaryFile(max_size=_SPOOL_MEMORY_LIMIT, mode='w+b') as staged:
            while True:
                chunk = content.read(_IO_CHUNK_SIZE)
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    raise TypeError('content.read() must return bytes')
                if not chunk:
                    break
                digest.update(chunk)
                size += len(chunk)
                staged.write(chunk)
            reference = self._new_reference(instance_id, size, digest.hexdigest())
            staged.seek(0)
            self._client.set_binary_file_alpha1(
                reference.store_name, reference.key, staged, overwrite=True
            )
        return cast(PayloadReference[bytes], reference)

    def resolve_bytes(self, reference: object) -> bytes:
        """Download and integrity-check bytes, enforcing the configured buffer limit."""
        parsed = self._coerce_reference(reference)
        self._ensure_supported_stages(parsed)
        self._check_rehydration_size(parsed)
        chunks: list[bytes] = []
        size = 0
        with self.open(parsed) as response:
            for chunk in response:
                size += len(chunk)
                limit = self.max_rehydration_size
                if limit is not None and size > limit:
                    raise PayloadRehydrationLimitError(parsed, limit)
                chunks.append(chunk)
        return b''.join(chunks)

    def resolve(self, reference: object) -> Any:
        """Download, integrity-check, and deserialize a stored workflow value."""
        return self._deserialize_value(self.resolve_bytes(reference))

    @contextmanager
    def open(self, reference: object) -> Iterator[Iterator[bytes]]:
        """Open raw bytes as a verified stream; integrity is checked when consumed to EOF."""
        parsed = self._coerce_reference(reference)
        self._ensure_supported_stages(parsed)
        digest = hashlib.sha256()
        size = 0
        with self._client.get_binary_file_alpha1(parsed.store_name, parsed.key) as response:
            def verified_chunks() -> Iterator[bytes]:
                nonlocal size
                for chunk in response:
                    size += len(chunk)
                    digest.update(chunk)
                    yield chunk
                self._verify_payload(parsed, size, digest.hexdigest())

            yield verified_chunks()

    def delete(self, reference: object) -> None:
        """Delete one explicitly selected payload. Deletion is never automatic."""
        parsed = self._coerce_reference(reference)
        self._ensure_supported_stages(parsed)
        self._client.delete_binary_file_alpha1(parsed.store_name, parsed.key)
