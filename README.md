# haphazard

A small, focused Rust implementation of hazard pointers for lock-free memory reclamation.

This crate provides a low-level hazard pointer domain, holders, and helpers to safely protect and retire heap-allocated objects used by lock-free data structures.

## What this crate provides

- Domain: `Domain<F>` — manages hazard pointer records and retired object lists.
- Global domain: `Global` and `Domain::global()` — a shared domain instance for common use.
- Hazard pointer holders: `HazPtrHolder<'domain, F>` — acquires a hazard pointer record from a domain and can protect pointers while reading.
- HazPtr objects: `HazPtrObject<'domain, F>` and `HazPtrObjectWrapper<'domain, T, F>` — helpers to associate heap objects with a domain and retire them safely.
- Reclamation: `Deleter` trait and the `deleters` module — pluggable deletion strategies (examples: `drop_in_place`, `drop_box`).

See the `src/` directory for the concrete implementation:
- `src/domain.rs` — domain and reclamation logic (sharded retired lists, reclamation loops, thresholds)
- `src/holder.rs` — API for protecting and accessing pointers with hazard pointers
- `src/object.rs` — object wrapper and trait for domain-associated objects
- `src/record.rs` — hazard pointer record structure
- `src/deleter.rs` — deletion strategies and the `Deleter` trait

## Quick example

The crate is low-level and uses `unsafe` where appropriate. This example shows the reader-side usage pattern with a global domain and an atomic pointer to a domain-associated object.

```rust
use std::sync::atomic::AtomicPtr;
use haphazard::*;

// Writer: allocate object and publish via AtomicPtr
let boxed = Box::new(HazPtrObjectWrapper::with_global_domain(42usize));
let atomic: AtomicPtr<_> = AtomicPtr::new(Box::into_raw(Box::new(boxed)));

// Reader: protect and load
let mut holder = HazPtrHolder::global();
let opt_ref = unsafe { holder.protect(&atomic) };
if let Some(r) = opt_ref {
    // use `r` which derefs to the inner value
    println!("value = {}", *r);
}

// When removing an object, retire it with a deleter
// unsafe { HazPtrObjectWrapper::retire(ptr, &deleters::drop_box) };
```

(See the source for a correct end-to-end example — the API is intentionally minimal and unsafe operations require careful use.)

## Safety notes

- This crate uses `unsafe` extensively where necessary for lock-free reclamation. Callers must follow the documented patterns (protect pointers with `HazPtrHolder` before accessing) to avoid use-after-free.
- Objects must implement `HazPtrObject`/be wrapped with `HazPtrObjectWrapper` so they can be retired into the proper domain.
- Deletions are performed via the `Deleter` trait; choose an appropriate deleter (`deleters::drop_box` or `deleters::drop_in_place`) depending on how objects were allocated.

## Building and testing

Requires recent Rust (edition 2024 as declared in Cargo.toml).

- Build: `cargo build`
- Test: `cargo test`

## Limitations and notes

- The crate is still small and under active development; the implementation supports single-item retire pushes and has conservative reclamation thresholds.
- No explicit runtime feature flags or std/no-std support are currently provided.

## Contributing

Contributions, bug reports, and suggestions are welcome. Please open issues or PRs on the repository.

## License

No license file was found in the repository. Please add a LICENSE file or clarify the intended license.
