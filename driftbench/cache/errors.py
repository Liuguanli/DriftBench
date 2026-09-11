from __future__ import annotations


class RemoteCacheError(RuntimeError):
    """Base class for safe, user-facing remote cache failures."""


class CacheConfigurationError(RemoteCacheError, ValueError):
    """The request or non-secret cache configuration is invalid."""


class CacheCredentialError(RemoteCacheError):
    """No usable credential was available or authentication failed."""


class CacheAuthenticationError(RemoteCacheError):
    """Azure rejected a credential presented by the configured identity chain."""


class CacheAuthorizationError(RemoteCacheError):
    """The authenticated principal lacks required data-plane access."""


class CacheTransportError(RemoteCacheError):
    """The remote service could not be reached or complete an operation."""


class CacheIntegrityError(RemoteCacheError):
    """A cache entry failed schema, containment, size, or digest validation."""


class CacheCollisionError(CacheIntegrityError):
    """An immutable key already exists with different content."""


class CacheGenerationError(RemoteCacheError):
    """The local artifact generator failed."""


class RemoteObjectNotFound(CacheTransportError):
    """An authenticated exact-path request returned HTTP 404."""


class RemoteObjectConflict(CacheTransportError):
    """A non-overwriting create or rename encountered HTTP 409/412."""


class AzureDependencyError(CacheConfigurationError):
    """The optional Azure SDK extra is not installed."""
