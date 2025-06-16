# Python Client API

## Definitions

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
```

## Examples

```python

# defined statically:
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
