from __future__ import annotations

import contextlib
import hashlib
import json
import os
import shutil
import stat
import tempfile
import time
import uuid
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Any, Iterator

from driftbench.data.base import GenerationResult

from .backend import RemoteCacheBackend
from .errors import (
    CacheCollisionError,
    CacheConfigurationError,
    CacheGenerationError,
    CacheIntegrityError,
    CacheTransportError,
    RemoteObjectConflict,
    RemoteObjectNotFound,
)
from .identity import (
    ArtifactIdentity,
    REMOTE_CACHE_SCHEMA,
    REMOTE_CACHE_VERSION,
    REMOTE_MANIFEST_SCHEMA,
    identity_for,
)
from .models import (
    AzureHNSCacheConfig,
    MaterializationResult,
    RemoteCacheMode,
    validate_posix_relative_path,
)


_LOCAL_CACHE_SCHEMA = "driftbench.benchmark-cache"
_LOCAL_CACHE_VERSION = 3
_REMOTE_MANIFEST_LIMIT = 4 * 1024 * 1024
_LOCK_TIMEOUT_SECONDS = 10.0
_HASH_CHUNK_BYTES = 1024 * 1024


def _join_remote(*parts: str) -> str:
    return "/".join(part.strip("/") for part in parts if part.strip("/"))


def _entry_path(config: AzureHNSCacheConfig, identity: ArtifactIdentity) -> str:
    return _join_remote(
        config.prefix,
        "cache",
        REMOTE_CACHE_SCHEMA,
        REMOTE_CACHE_VERSION,
        identity.benchmark,
        identity.artifact_type,
        identity.generator_id,
        identity.fingerprint,
    )


def _read_remote_bytes(
    backend: RemoteCacheBackend,
    path: str,
    *,
    limit: int | None = None,
) -> bytes:
    payload = bytearray()
    for chunk in backend.iter_bytes(path):
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise CacheIntegrityError("remote backend returned a non-byte payload")
        if limit is not None and len(payload) + len(chunk) > limit:
            raise CacheIntegrityError("remote metadata exceeds the allowed size")
        payload.extend(chunk)
    return bytes(payload)


def _sha256_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(_HASH_CHUNK_BYTES):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def _is_portably_executable(path: Path) -> bool:
    return bool(path.stat().st_mode & stat.S_IXUSR)


def _safe_relative_path(path: Path, root: Path, *, field: str) -> str:
    if path.is_symlink() or not path.is_file():
        raise CacheIntegrityError(f"{field} must be a non-symlink regular file")
    try:
        relative = path.resolve(strict=True).relative_to(root.resolve())
    except (FileNotFoundError, ValueError) as exc:
        raise CacheIntegrityError(f"{field} is missing or escapes the output root") from exc
    value = relative.as_posix()
    validate_posix_relative_path(value, field=field)
    return value


def _validate_remote_relative_path(value: Any, *, field: str) -> str:
    try:
        return validate_posix_relative_path(value, field=field)
    except CacheConfigurationError as exc:
        raise CacheIntegrityError(f"{field} is unsafe") from exc


def _expected_local_cache_record(identity: ArtifactIdentity) -> dict[str, Any]:
    canonical = {
        "schema": _LOCAL_CACHE_SCHEMA,
        "version": _LOCAL_CACHE_VERSION,
        "generator": f"{identity.generator}/{identity.artifact_type}",
        "parameters": identity.parameters,
    }
    encoded = json.dumps(
        canonical,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return {
        **canonical,
        "fingerprint": hashlib.sha256(encoded).hexdigest(),
    }


def _validate_local_generation(
    generation: GenerationResult,
    identity: ArtifactIdentity,
    output_root: Path,
) -> list[dict[str, Any]]:
    if generation.benchmark != identity.benchmark or generation.artifact_type != identity.artifact_type:
        raise CacheIntegrityError("generator returned the wrong benchmark or artifact type")
    if generation.output_dir.resolve() != output_root.resolve():
        raise CacheIntegrityError("generator returned an unexpected output root")
    if not generation.files:
        raise CacheIntegrityError("generator returned no managed artifacts")

    metadata_candidate = Path(generation.metadata)
    metadata_relative = _safe_relative_path(
        metadata_candidate, output_root, field="metadata path"
    )
    metadata_path = metadata_candidate.resolve(strict=True)
    try:
        local_manifest = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CacheIntegrityError("generator metadata is not readable JSON") from exc
    if not isinstance(local_manifest, dict):
        raise CacheIntegrityError("generator metadata must be a JSON object")
    if local_manifest.get("benchmark") != identity.benchmark or local_manifest.get(
        "artifact_type"
    ) != identity.artifact_type:
        raise CacheIntegrityError("generator metadata identity does not match the request")

    cache = local_manifest.get("cache")
    if not isinstance(cache, dict) or set(cache) != {
        "schema",
        "version",
        "generator",
        "parameters",
        "fingerprint",
        "artifacts",
    }:
        raise CacheIntegrityError("generator metadata is not a cache-v3 manifest")
    cache_identity = dict(cache)
    cache_identity.pop("artifacts")
    if cache_identity != _expected_local_cache_record(identity):
        raise CacheIntegrityError(
            "generator metadata does not match the exact artifact identity"
        )

    manifest_paths = local_manifest.get("files")
    artifacts = cache.get("artifacts")
    if not isinstance(manifest_paths, list) or not isinstance(artifacts, list):
        raise CacheIntegrityError("generator metadata has an invalid managed file set")
    if len(manifest_paths) != len(generation.files) or len(artifacts) != len(generation.files):
        raise CacheIntegrityError("generator metadata file count does not match the result")

    descriptors: list[dict[str, Any]] = []
    seen: set[str] = set()
    for result_path, manifest_path, local_descriptor in zip(
        generation.files, manifest_paths, artifacts
    ):
        if not isinstance(manifest_path, str) or not isinstance(local_descriptor, dict):
            raise CacheIntegrityError("generator metadata contains an invalid file descriptor")
        relative = _safe_relative_path(Path(result_path), output_root, field="artifact path")
        if relative != manifest_path.replace("\\", "/"):
            raise CacheIntegrityError("generator result and metadata paths differ")
        validate_posix_relative_path(relative, field="artifact path")
        folded = relative.casefold()
        if folded in seen:
            raise CacheIntegrityError("generator paths collide case-insensitively")
        seen.add(folded)
        if set(local_descriptor) != {"path", "bytes", "sha256"}:
            raise CacheIntegrityError("generator metadata descriptor shape is invalid")
        size, digest = _sha256_file(Path(result_path))
        if (
            local_descriptor.get("path", "").replace("\\", "/") != relative
            or local_descriptor.get("bytes") != size
            or local_descriptor.get("sha256") != digest
        ):
            raise CacheIntegrityError("generator artifact does not match its local manifest")
        descriptors.append(
            {
                "path": relative,
                "role": "artifact",
                "bytes": size,
                "sha256": digest,
                "executable": _is_portably_executable(Path(result_path)),
            }
        )

    if metadata_relative.casefold() in seen:
        raise CacheIntegrityError("metadata path collides with a managed artifact")
    metadata_size, metadata_digest = _sha256_file(metadata_path)
    descriptors.append(
        {
            "path": metadata_relative,
            "role": "metadata",
            "bytes": metadata_size,
            "sha256": metadata_digest,
            "executable": _is_portably_executable(metadata_path),
        }
    )

    subtree = PurePosixPath(identity.benchmark, identity.artifact_type)
    for descriptor in descriptors:
        try:
            PurePosixPath(descriptor["path"]).relative_to(subtree)
        except ValueError as exc:
            raise CacheIntegrityError("generator path escapes its managed adapter subtree") from exc
    return descriptors


def _remote_manifest(
    identity: ArtifactIdentity,
    descriptors: list[dict[str, Any]],
) -> dict[str, Any]:
    identity_payload = identity.canonical()
    identity_payload.update(
        {
            "fingerprint": identity.fingerprint,
            "generator_id": identity.generator_id,
        }
    )
    return {
        "schema": REMOTE_MANIFEST_SCHEMA,
        "version": 1,
        "identity": identity_payload,
        "output_subtree": f"{identity.benchmark}/{identity.artifact_type}",
        "files": descriptors,
    }


def _encode_json(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _validate_remote_manifest(
    payload: Any,
    identity: ArtifactIdentity,
) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or set(payload) != {
        "schema",
        "version",
        "identity",
        "output_subtree",
        "files",
    }:
        raise CacheIntegrityError("remote manifest shape is invalid")
    if payload["schema"] != REMOTE_MANIFEST_SCHEMA or payload["version"] != 1:
        raise CacheIntegrityError("remote manifest schema/version is unsupported")

    expected_identity = identity.canonical()
    expected_identity.update(
        {"fingerprint": identity.fingerprint, "generator_id": identity.generator_id}
    )
    if payload["identity"] != expected_identity:
        raise CacheIntegrityError("remote manifest identity does not match the exact request")
    expected_subtree = f"{identity.benchmark}/{identity.artifact_type}"
    if payload["output_subtree"] != expected_subtree:
        raise CacheIntegrityError("remote manifest output subtree is invalid")

    files = payload["files"]
    if not isinstance(files, list) or len(files) < 2:
        raise CacheIntegrityError("remote manifest has no complete artifact set")
    seen: set[str] = set()
    metadata_count = 0
    artifact_count = 0
    for descriptor in files:
        if not isinstance(descriptor, dict) or set(descriptor) != {
            "path",
            "role",
            "bytes",
            "sha256",
            "executable",
        }:
            raise CacheIntegrityError("remote file descriptor shape is invalid")
        path = descriptor["path"]
        _validate_remote_relative_path(path, field="remote artifact path")
        try:
            PurePosixPath(path).relative_to(PurePosixPath(expected_subtree))
        except ValueError as exc:
            raise CacheIntegrityError("remote artifact path escapes its adapter subtree") from exc
        folded = path.casefold()
        if folded in seen:
            raise CacheIntegrityError("remote artifact paths collide case-insensitively")
        seen.add(folded)
        role = descriptor["role"]
        metadata_count += int(role == "metadata")
        artifact_count += int(role == "artifact")
        if role not in {"artifact", "metadata"}:
            raise CacheIntegrityError("remote artifact role is invalid")
        if type(descriptor["bytes"]) is not int or descriptor["bytes"] < 0:
            raise CacheIntegrityError("remote artifact size is invalid")
        if type(descriptor["executable"]) is not bool:
            raise CacheIntegrityError("remote executable flag is invalid")
        digest = descriptor["sha256"]
        if not isinstance(digest, str) or len(digest) != 64:
            raise CacheIntegrityError("remote artifact digest is invalid")
        try:
            int(digest, 16)
        except ValueError as exc:
            raise CacheIntegrityError("remote artifact digest is invalid") from exc
        if digest != digest.lower():
            raise CacheIntegrityError("remote artifact digest must be lowercase")
    if metadata_count != 1 or artifact_count < 1:
        raise CacheIntegrityError("remote manifest requires one metadata file and artifacts")
    return files


def _load_remote_manifest(
    backend: RemoteCacheBackend,
    entry: str,
    identity: ArtifactIdentity,
    *,
    allow_miss: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]], bytes] | None:
    manifest_path = _join_remote(entry, "manifest.json")
    try:
        encoded = _read_remote_bytes(
            backend, manifest_path, limit=_REMOTE_MANIFEST_LIMIT
        )
    except RemoteObjectNotFound:
        if allow_miss:
            return None
        raise CacheIntegrityError("remote cache entry disappeared during verification")
    try:
        payload = json.loads(encoded.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CacheIntegrityError("remote manifest is not valid UTF-8 JSON") from exc
    descriptors = _validate_remote_manifest(payload, identity)

    try:
        commit_encoded = _read_remote_bytes(
            backend,
            _join_remote(entry, "_COMMITTED.json"),
            limit=64 * 1024,
        )
    except RemoteObjectNotFound as exc:
        raise CacheIntegrityError("remote cache entry has no commit marker") from exc
    try:
        commit = json.loads(commit_encoded.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CacheIntegrityError("remote commit marker is invalid") from exc
    expected_commit = {
        "schema": "driftbench.remote-cache-commit/v1",
        "fingerprint": identity.fingerprint,
        "manifest_sha256": hashlib.sha256(encoded).hexdigest(),
    }
    if commit != expected_commit:
        raise CacheIntegrityError("remote commit marker does not authenticate the manifest bytes")
    return payload, descriptors, encoded


def _verify_remote_file(
    backend: RemoteCacheBackend,
    path: str,
    descriptor: dict[str, Any],
) -> None:
    digest = hashlib.sha256()
    size = 0
    try:
        chunks = backend.iter_bytes(path)
        for chunk in chunks:
            if size + len(chunk) > descriptor["bytes"]:
                raise CacheIntegrityError(
                    "remote artifact exceeds the size declared by the manifest"
                )
            digest.update(chunk)
            size += len(chunk)
    except RemoteObjectNotFound as exc:
        raise CacheIntegrityError("remote cache entry is missing a declared artifact") from exc
    if size != descriptor["bytes"] or digest.hexdigest() != descriptor["sha256"]:
        raise CacheIntegrityError("remote artifact size or SHA-256 does not match the manifest")


@contextlib.contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    """Acquire an OS-backed lock that is released automatically on process exit."""

    deadline = time.monotonic() + _LOCK_TIMEOUT_SECONDS
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if _is_link_like(path.parent) or not path.parent.is_dir():
        raise CacheIntegrityError("local lock root is not a safe directory")

    descriptor = os.open(str(path), os.O_CREAT | os.O_RDWR, 0o600)
    acquired = False
    try:
        if _is_link_like(path) or not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise CacheIntegrityError("local lock path is not a regular file")

        while not acquired:
            try:
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":
                    import msvcrt

                    # msvcrt locks an existing byte. Initialize it inside the
                    # retry loop because another caller can create, initialize,
                    # and lock the shared file after this descriptor is opened.
                    if os.fstat(descriptor).st_size < 1:
                        os.write(descriptor, b"\0")
                        os.lseek(descriptor, 0, os.SEEK_SET)
                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except OSError as exc:
                if time.monotonic() >= deadline:
                    raise CacheTransportError(
                        "timed out waiting for the local materialization lock"
                    ) from exc
                time.sleep(0.05)
        yield
    finally:
        if acquired:
            try:
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                # Closing the descriptor releases either platform lock even if
                # an explicit unlock reports a transient error.
                pass
        os.close(descriptor)


def _materialization_lock_path(
    output_root: Path,
    benchmark: str,
    artifact_type: str,
) -> Path:
    encoded = (
        f"{output_root.resolve()}\0{benchmark}\0{artifact_type}"
    ).encode("utf-8")
    name = hashlib.sha256(encoded).hexdigest() + ".lock"
    return (
        Path(tempfile.gettempdir()).resolve()
        / "driftbench-materialization-locks-v1"
        / name
    )


def _adapter_lock_scope(adapter: object) -> tuple[str, str]:
    """Return a stable local-write scope without requiring remote eligibility."""

    adapter_type = type(adapter)
    type_name = f"{adapter_type.__module__}.{adapter_type.__qualname__}"
    benchmark = getattr(adapter, "benchmark", None)
    artifact_type = getattr(adapter, "artifact_type", None)
    return (
        benchmark if isinstance(benchmark, str) and benchmark else type_name,
        artifact_type
        if isinstance(artifact_type, str) and artifact_type
        else "materialize",
    )


def _is_link_like(path: Path) -> bool:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    attributes = getattr(info, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    is_junction = getattr(os.path, "isjunction", lambda _path: False)
    return (
        stat.S_ISLNK(info.st_mode)
        or bool(attributes & reparse_flag)
        or bool(is_junction(path))
    )


def _lexists(path: Path) -> bool:
    return os.path.lexists(path)


def _relative_without_resolving(path: Path, parent: Path) -> Path:
    try:
        return Path(os.path.abspath(path)).relative_to(Path(os.path.abspath(parent)))
    except ValueError as exc:
        raise CacheIntegrityError("managed cache path escaped its trusted root") from exc


def _assert_owned_sibling(path: Path, parent: Path, prefix: str) -> None:
    relative = _relative_without_resolving(path, parent)
    if len(relative.parts) != 1 or not relative.name.startswith(prefix):
        raise CacheIntegrityError("owned staging path has an unexpected name")


def _ensure_safe_directory(path: Path, anchor: Path) -> None:
    relative = _relative_without_resolving(path, anchor)
    if _is_link_like(anchor) or not anchor.is_dir():
        raise CacheIntegrityError("managed output root is not a safe directory")
    current = anchor
    for part in relative.parts:
        current = current / part
        if _lexists(current):
            if _is_link_like(current) or not current.is_dir():
                raise CacheIntegrityError(
                    "managed output path contains a symlink, junction, or reparse point"
                )
        else:
            current.mkdir()
        if _is_link_like(current) or not current.is_dir():
            raise CacheIntegrityError(
                "managed output path changed while establishing containment"
            )


def _assert_safe_tree(path: Path, anchor: Path) -> None:
    _relative_without_resolving(path, anchor)
    if _is_link_like(path) or not path.is_dir():
        raise CacheIntegrityError(
            "managed cache tree is not a non-reparse directory"
        )
    for current_value, directory_names, file_names in os.walk(
        path, topdown=True, followlinks=False
    ):
        current = Path(current_value)
        _relative_without_resolving(current, anchor)
        for name in directory_names:
            candidate = current / name
            if _is_link_like(candidate):
                raise CacheIntegrityError(
                    "managed cache tree contains a symlink, junction, or reparse point"
                )
        for name in file_names:
            candidate = current / name
            if _is_link_like(candidate) or not candidate.is_file():
                raise CacheIntegrityError(
                    "managed cache tree contains a non-regular file"
                )


def _remove_owned_tree(path: Path, parent: Path, prefix: str) -> None:
    if not _lexists(path):
        return
    _assert_owned_sibling(path, parent, prefix)
    _assert_safe_tree(path, parent)
    shutil.rmtree(path)


def _local_subtree_matches(
    final_subtree: Path,
    descriptors: list[dict[str, Any]],
    subtree: PurePosixPath,
) -> bool:
    if _lexists(final_subtree) and (
        _is_link_like(final_subtree) or not final_subtree.is_dir()
    ):
        raise CacheIntegrityError(
            "managed adapter output path is a symlink, junction, reparse point, or non-directory"
        )
    if not final_subtree.is_dir():
        return False
    _assert_safe_tree(final_subtree, final_subtree.parent)
    expected_paths = {
        PurePosixPath(descriptor["path"]).relative_to(subtree).as_posix().casefold()
        for descriptor in descriptors
    }
    observed_paths: set[str] = set()
    for current_value, _, file_names in os.walk(
        final_subtree, topdown=True, followlinks=False
    ):
        current = Path(current_value)
        for name in file_names:
            candidate = current / name
            relative = candidate.relative_to(final_subtree).as_posix().casefold()
            if relative in observed_paths:
                return False
            observed_paths.add(relative)
    if observed_paths != expected_paths:
        return False
    for descriptor in descriptors:
        relative = PurePosixPath(descriptor["path"]).relative_to(subtree)
        candidate = final_subtree.joinpath(*relative.parts)
        if not candidate.is_file() or candidate.is_symlink():
            return False
        size, digest = _sha256_file(candidate)
        if (
            size != descriptor["bytes"]
            or digest != descriptor["sha256"]
            or _is_portably_executable(candidate) != descriptor["executable"]
        ):
            return False
    return True


def _materialize_hit(
    backend: RemoteCacheBackend,
    entry: str,
    identity: ArtifactIdentity,
    descriptors: list[dict[str, Any]],
    output_root: Path,
) -> GenerationResult:
    subtree = PurePosixPath(identity.benchmark, identity.artifact_type)
    output_root.mkdir(parents=True, exist_ok=True)
    if _is_link_like(output_root) or not output_root.is_dir():
        raise CacheIntegrityError("managed output root is not a safe directory")
    final_parent = output_root / identity.benchmark
    final_subtree = final_parent / identity.artifact_type
    _ensure_safe_directory(final_parent, output_root)
    if _lexists(final_subtree) and (
        _is_link_like(final_subtree) or not final_subtree.is_dir()
    ):
        raise CacheIntegrityError(
            "managed adapter output path is a symlink, junction, reparse point, or non-directory"
        )

    staging_prefix = ".driftbench-cache-pull-"
    staging_root = output_root / f"{staging_prefix}{uuid.uuid4().hex}"
    _assert_owned_sibling(staging_root, output_root, staging_prefix)
    staging_root.mkdir()

    try:
        artifact_files: list[Path] = []
        metadata: Path | None = None
        for descriptor in descriptors:
            relative = PurePosixPath(descriptor["path"])
            destination = staging_root.joinpath(*relative.parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256()
            size = 0
            try:
                with destination.open("xb") as stream:
                    for chunk in backend.iter_bytes(
                        _join_remote(entry, "artifacts", descriptor["path"])
                    ):
                        if size + len(chunk) > descriptor["bytes"]:
                            raise CacheIntegrityError(
                                "downloaded artifact exceeds the size declared by the manifest"
                            )
                        stream.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)
            except RemoteObjectNotFound as exc:
                raise CacheIntegrityError(
                    "remote cache entry is missing a declared artifact"
                ) from exc
            if size != descriptor["bytes"] or digest.hexdigest() != descriptor["sha256"]:
                raise CacheIntegrityError(
                    "downloaded artifact size or SHA-256 does not match the manifest"
                )
            if descriptor["executable"]:
                destination.chmod(0o755)
            if descriptor["role"] == "metadata":
                metadata = destination
            else:
                artifact_files.append(destination)

        if metadata is None:
            raise CacheIntegrityError("materialized cache entry has no metadata file")
        staged_generation = GenerationResult(
            benchmark=identity.benchmark,
            artifact_type=identity.artifact_type,
            output_dir=staging_root,
            files=artifact_files,
            metadata=metadata,
        )
        staged_descriptors = _validate_local_generation(
            staged_generation, identity, staging_root
        )
        if staged_descriptors != descriptors:
            raise CacheIntegrityError(
                "downloaded inner manifest does not match the outer cache manifest"
            )
        staging_subtree = staging_root / identity.benchmark / identity.artifact_type
        _assert_safe_tree(staging_subtree, staging_root)

        _ensure_safe_directory(final_parent, output_root)
        if _local_subtree_matches(final_subtree, descriptors, subtree):
            pass
        else:
            backup = final_parent / f".{identity.artifact_type}.cache-backup-{uuid.uuid4().hex}"
            _assert_owned_sibling(
                backup, final_parent, f".{identity.artifact_type}.cache-backup-"
            )
            moved_existing = False
            try:
                if _lexists(final_subtree):
                    _assert_safe_tree(final_subtree, final_parent)
                    os.replace(final_subtree, backup)
                    moved_existing = True
                os.replace(staging_subtree, final_subtree)
            except Exception:
                if (
                    moved_existing
                    and _lexists(backup)
                    and not _lexists(final_subtree)
                ):
                    _assert_safe_tree(backup, final_parent)
                    os.replace(backup, final_subtree)
                raise
            if _lexists(backup):
                _remove_owned_tree(
                    backup,
                    final_parent,
                    f".{identity.artifact_type}.cache-backup-",
                )
    finally:
        _remove_owned_tree(staging_root, output_root, staging_prefix)

    artifact_files: list[Path] = []
    metadata: Path | None = None
    for descriptor in descriptors:
        relative = PurePosixPath(descriptor["path"]).relative_to(subtree)
        final_path = final_subtree.joinpath(*relative.parts)
        if descriptor["role"] == "metadata":
            metadata = final_path
        else:
            artifact_files.append(final_path)
    if metadata is None:
        raise CacheIntegrityError("materialized cache entry has no metadata file")
    generation = GenerationResult(
        benchmark=identity.benchmark,
        artifact_type=identity.artifact_type,
        output_dir=output_root,
        files=artifact_files,
        metadata=metadata,
    )
    if _validate_local_generation(generation, identity, output_root) != descriptors:
        raise CacheIntegrityError("materialized local cache entry changed after publication")
    return generation


def _upload_and_verify(
    backend: RemoteCacheBackend,
    entry: str,
    identity: ArtifactIdentity,
    generation: GenerationResult,
    descriptors: list[dict[str, Any]],
) -> str:
    entry_parent = entry.rsplit("/", 1)[0]
    staging = _join_remote(
        entry_parent,
        "_staging",
        f"{identity.fingerprint}-{uuid.uuid4().hex}",
    )
    manifest = _remote_manifest(identity, descriptors)
    manifest_encoded = _encode_json(manifest)
    commit_encoded = _encode_json(
        {
            "schema": "driftbench.remote-cache-commit/v1",
            "fingerprint": identity.fingerprint,
            "manifest_sha256": hashlib.sha256(manifest_encoded).hexdigest(),
        }
    )

    renamed = False
    try:
        paths_by_relative = {
            _safe_relative_path(path, generation.output_dir, field="artifact path"): Path(path)
            for path in generation.files
        }
        paths_by_relative[
            _safe_relative_path(generation.metadata, generation.output_dir, field="metadata path")
        ] = generation.metadata
        for descriptor in descriptors:
            backend.upload_file(
                paths_by_relative[descriptor["path"]],
                _join_remote(staging, "artifacts", descriptor["path"]),
            )
            _verify_remote_file(
                backend,
                _join_remote(staging, "artifacts", descriptor["path"]),
                descriptor,
            )
        backend.upload_bytes(manifest_encoded, _join_remote(staging, "manifest.json"))
        if _read_remote_bytes(
            backend, _join_remote(staging, "manifest.json"), limit=_REMOTE_MANIFEST_LIMIT
        ) != manifest_encoded:
            raise CacheIntegrityError("uploaded remote manifest bytes changed")
        backend.upload_bytes(commit_encoded, _join_remote(staging, "_COMMITTED.json"))
        if _read_remote_bytes(
            backend, _join_remote(staging, "_COMMITTED.json"), limit=64 * 1024
        ) != commit_encoded:
            raise CacheIntegrityError("uploaded remote commit marker bytes changed")
        try:
            backend.rename_directory(staging, entry)
            renamed = True
            return "uploaded"
        except RemoteObjectConflict:
            winner = _load_remote_manifest(
                backend, entry, identity, allow_miss=False
            )
            assert winner is not None
            _, winner_descriptors, _ = winner
            if winner_descriptors != descriptors:
                raise CacheCollisionError(
                    "immutable remote key is occupied by different artifact descriptors"
                )
            for descriptor in winner_descriptors:
                _verify_remote_file(
                    backend,
                    _join_remote(entry, "artifacts", descriptor["path"]),
                    descriptor,
                )
            return "concurrent_identical"
    finally:
        if not renamed:
            try:
                backend.delete_directory(staging)
            except RemoteObjectNotFound:
                pass


def _build_backend(config: AzureHNSCacheConfig) -> RemoteCacheBackend:
    from .adls import AzureDataLakeBackend

    return AzureDataLakeBackend(config)


def _local_materialization_source(generation: GenerationResult) -> str:
    return "local_cache" if generation.reused_local else "generated"


def _local_cache_outcome(
    *,
    prefix: str,
    materialization_source: str,
    uploaded: bool = False,
) -> str:
    source = "local_cache" if materialization_source == "local_cache" else "generated"
    suffix = "_uploaded" if uploaded else ""
    return f"{prefix}_{source}{suffix}"


def _materialize_enabled(
    *,
    adapter: Any,
    output_root: Path,
    mode: RemoteCacheMode,
    force: bool,
    backend: RemoteCacheBackend,
    identity: ArtifactIdentity,
    entry: str,
) -> MaterializationResult:
    if not force:
        remote = _load_remote_manifest(backend, entry, identity, allow_miss=True)
        if remote is not None:
            _, descriptors, _ = remote
            generation = _materialize_hit(
                backend, entry, identity, descriptors, output_root
            )
            return MaterializationResult(
                generation=generation,
                cache_mode=mode,
                cache_outcome="hit",
                materialization_source="remote_cache",
                remote_entry_status="hit",
                force=force,
                remote_path=entry,
            )

    # Reaching this point means either the exact remote identity was absent or
    # the caller deliberately bypassed the remote probe. A cache-v3 local
    # manifest does not bind producer/generator revisions, so reusing it here
    # could publish old bytes under a newer immutable remote identity.
    try:
        generation = adapter.generate(output_dir=output_root, force=True)
    except Exception as exc:
        raise CacheGenerationError("local artifact generation failed") from exc
    if generation.reused_local:
        raise CacheIntegrityError(
            "forced local generation unexpectedly reported local cache reuse"
        )
    descriptors = _validate_local_generation(generation, identity, output_root)
    source = _local_materialization_source(generation)

    if mode is RemoteCacheMode.READ:
        prefix = "force" if force else "miss"
        return MaterializationResult(
            generation=generation,
            cache_mode=mode,
            cache_outcome=_local_cache_outcome(
                prefix=prefix, materialization_source=source
            ),
            materialization_source=source,
            remote_entry_status="not_checked" if force else "miss",
            force=force,
            remote_path=entry,
        )

    publish_status = _upload_and_verify(
        backend, entry, identity, generation, descriptors
    )
    if publish_status == "concurrent_identical":
        outcome = publish_status
    else:
        outcome = _local_cache_outcome(
            prefix="force" if force else "miss",
            materialization_source=source,
            uploaded=True,
        )
    return MaterializationResult(
        generation=generation,
        cache_mode=mode,
        cache_outcome=outcome,
        materialization_source=source,
        remote_entry_status=publish_status,
        force=force,
        remote_path=entry,
    )


def materialize_artifacts(
    *,
    adapter: Any,
    output_dir: str | Path,
    remote_cache: AzureHNSCacheConfig | None = None,
    force: bool = False,
    _backend: RemoteCacheBackend | None = None,
) -> MaterializationResult:
    """Materialize one registered adapter with an optional immutable HNS cache."""

    if type(force) is not bool:
        raise CacheConfigurationError("force must be a boolean")
    config = remote_cache or AzureHNSCacheConfig()
    mode = config.normalized_mode()
    output_root = Path(output_dir).expanduser().resolve()
    lock_scope = _adapter_lock_scope(adapter)
    lock_path = _materialization_lock_path(output_root, *lock_scope)

    if mode is RemoteCacheMode.OFF:
        with _exclusive_lock(lock_path):
            try:
                generation = adapter.generate(output_dir=output_root, force=force)
            except Exception as exc:
                raise CacheGenerationError("local artifact generation failed") from exc
            source = _local_materialization_source(generation)
            return MaterializationResult(
                generation=generation,
                cache_mode=mode,
                cache_outcome=_local_cache_outcome(
                    prefix="disabled", materialization_source=source
                ),
                materialization_source=source,
                remote_entry_status="not_checked",
                force=force,
            )

    config.validate_enabled()
    identity = identity_for(adapter)
    entry = _entry_path(config, identity)
    backend = _backend if _backend is not None else _build_backend(config)
    with _exclusive_lock(lock_path):
        return _materialize_enabled(
            adapter=adapter,
            output_root=output_root,
            mode=mode,
            force=force,
            backend=backend,
            identity=identity,
            entry=entry,
        )
