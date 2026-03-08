# DataFusionSharp Native

Rust FFI library providing DataFusion bindings for .NET. Compiles to a C dynamic library (`cdylib`) that the C# project loads via P/Invoke.

## Building

```bash
cargo build --profile dev
cargo build --profile release
```

Note: Normally built automatically via `dotnet build` from the parent project.

## Structure

- `lib.rs` - Module exports
- `runtime.rs` - Tokio async runtime management
- `context.rs` - DataFusion SessionContext wrapper
- `dataframe.rs` - DataFrame operations
- `callback.rs` - FFI callback mechanism for async operations
- `error.rs` - Error codes shared with C#

## Memory Rules

- **Handles:** Rust owns; C# calls destroy functions via `IDisposable`
- **Transient data:** Caller owns; callee copies if needed
