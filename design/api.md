# Client API

## Definitions

The most simple storage API, that just has put/get, but already has abstractions for usecase/scope isolation.

```python
class StorageServiceBuilder:
    @classmethod
    def for_usecase(cls, usecase: str) -> StorageService: ...

class StorageScope:
    @classmethod
    def for_organization(cls, org_id: int) -> StorageScope: ...

class StorageService:
    @classmethod
    def with_scope(cls, scope: StorageScope) -> StorageClient: ...

class StorageId:
    @classmethod
    def from_str(cls) -> StorageId: ...

class StorageClient:
    async def put_blob(self, contents: bytes | BytesIO, id: StorageId | None = None) -> StorageId: ...

    async def get_blob(self, id: StorageId) -> BytesIO: ...

    async def delete_blob(self, id: StorageId): ...
```

## Examples

One can use this API is such a way:

```python
# defined/configured statically:
ATTACHMENT_STORAGE = StorageServiceBuilder.for_usecase("attachments")

# used dynamically:
scope = StorageScope.for_organization(org_id)
storage = ATTACHMENT_STORAGE.with_scope(scope)

# storing attachments:
storage_id = await storage.put_blob(b"some raw attachment contents")
attachment.storage_id = storage_id
attachment.save()

# reading attachments:
contents = bytes(await storage.get_blob(attachment.storage_id)) # or however async streams work in python?
```

## Further API Evolution

Some other APIs which might offer things like "pre-signed put URLs" or chunked uploads, etc.

```rust
impl StorageClient {
    /// Generates a signed upload URL which can be used to upload a file which will later be available using the returned `StorageId`.
    async fn signed_put_url(self, id: Option<StorageId>, ttl: Option<Duration>) -> (Url, StorageId);

    /// Generates a short-lived signed download URL that allows external users to retrieve the file.
    /// 
    /// This could be used by the existing Sentry API, and returned as a `Location` header / HTTP redirect.
    async fn signed_get_url(self, id: StorageId, ttl: Option<Duration>) -> Url;

    /// Assembles a new file as a concatenation of the given parts.
    /// 
    /// This allows uploads similar to S3-style "Multipart" uploads, or Sentry-CLI chunked uploads.
    async fn assemble_from_parts(self, id: Option<StorageId>, parts: &[StorageId]) -> StorageId;

    /// For files stored with a TTI (time to idle), this refreshes its expiration time, without actually downloading the file.
    /// 
    /// Downloading the refreshes any TTI expiration time. However clients might want to avoid re-downloading files that are already cached.
    async fn keepalive(self, id: StorageId);
}
```

# Client-Server API

The intention is to have persistent connections between the Client and the Service.
Maybe we would use http2/protobuf (aka gRPC) for metadata, and HTTP3 for blob contents.

With this proposal, each upload/download would require 2 "roundtrips", but as we are dealing with persistent connections,
there should be minimal overhead.

```protobuf
edition = "2023";

// TODO: instead of using `string`/`string` here, maybe we want to "compress" the scope down to a single `u64`?
message Scope {
    string usecase = 1;
    string scope = 2;
}

// This is just an opaque newtype around a string/bytes for now
message StorageId {
    bytes id = 1;
}

// This internal call underlies the `put_blob` and `signed_put_url` client API calls.
message AllocateBlobRequest {
    Scope scope = 1;
    StorageId id = 2;
}
message AllocateBlobResponse {
    StorageId id = 1;
    string signed_put_url = 2;
}

// This internal call underlies the `get_blob` and `signed_get_url`, as well as `keepalive` client API calls.
message GetBlobLocationRequest {
    Scope scope = 1;
    StorageId id = 2;
}
message GetBlobLocationResponse {
    string signed_get_url = 1;
}

message AssembleFromPartsRequest {
    Scope scope = 1;
    StorageId id = 2;
    repeated StorageId parts = 3;
}
message AssembleFromPartsResponse {
    string signed_get_url = 1;
}
```

We would want to have compression in flight and at rest.
I would suggest to just move this to the client. The client would upload things zstd-compressed by default.
(This also means that users of the client-api do not need to care about compression at all).

TODO: Should we mandate `zstd` support from external clients?
If we want to be more backwards compatible to *external* clients that are downloading via `signed_get_url`,
we might decide to transparently decompress via `Accept-Encoding`.

```http
HTTP/3 PUT signed_put_url
Content-Encoding: zstd

...
```

```http
HTTP/3 GET signed_get_url
Accept-Encoding: zstd

...
```
