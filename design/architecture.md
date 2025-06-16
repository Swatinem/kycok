# Archtecture

## Concepts

### Usecase

The **usecase** would encapsulate higher-level capabilities, specific to a product/feature.

Capabilities can include things like TTL settings, quotas, etc.

The usecases would have to be provisioned directly in the infrastructure, and they would be exposed statically in client applications.

### Scope

The scope represents a partition and permission boundary.
We will most likely use the organization as the scope, but this can also be usecase specific, and be scoped to projects or repos instead.

As this scope represents a partition boundary, we can use this as a load-balancing and routing key.

### Ids / Keys

Some usecases, do not bring their own ids/keys/paths, and are fine with having ids allocated to them from the service itself.
This could just be a UUID, or whatever implementation is most efficient for the backend.

We might end up having to store an auxiliary table somewhere anyway to be able to do proper access control.

## Architectural Diagram

```mermaid
flowchart LR
    user["Client Library"]

    user --> service

    subgraph service ["Storage Service (load balanced)"]
        direction LR
        service1[[Service 1]]
        service2[[Service 2]]
        service3[[Service 3]]
    end

    subgraph backend ["Storage Backend"]
        direction LR
        master1[Master 1]@{ shape: database }
        master2[Master 2]@{ shape: database }
        master3[Master 3]@{ shape: database }

        master1 <--> master2
        master2 <--> master3
        master3 <--> master1

        volume1[Volume 1]@{ shape: disk }
        volume2[Volume 2]@{ shape: disk }
        volume3[Volume 3]@{ shape: disk }
        volumeN[Volume N]@{ shape: disk }

        volume1 ~~~ volume2
        volume2 ~~~ volume3
        volume3 -.- volumeN
    end

    db[Auxiliary Metadata]@{ shape: database }

    service --> backend
    service --> db
```
