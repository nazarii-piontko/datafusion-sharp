---
sidebar_position: 1
title: Installation
---

# Installation

## NuGet Package

```bash
dotnet add package DataFusionSharp
```

Or add to your `.csproj`:

```xml
<PackageReference Include="DataFusionSharp" Version="*" />
```

## Requirements

- **.NET 8.0** or later

## Platform Support

| Platform     | Architecture | Status |
|-------------|-------------|--------|
| Linux       | x64         | Supported |
| Linux       | arm64       | Supported |
| Windows     | x64         | Supported |
| macOS       | arm64       | Supported |

The native DataFusion library is bundled inside the NuGet package and loaded automatically at runtime — no additional setup is required.

## Building from Source

If you need to build the library from source:

### Prerequisites

- [.NET 10.0 SDK](https://learn.microsoft.com/en-us/dotnet/core/install/) or later
- [Rust 1.93+](https://rustup.rs) toolchain
- [protoc](https://protobuf.dev/installation/) (Protobuf compiler)

### Build Steps

```bash
git clone https://github.com/nazarii-piontko/datafusion-sharp.git
cd datafusion-sharp
dotnet build -c Release
```

This automatically compiles the Rust native library via Cargo, builds the .NET library, and links them together.

### Run Tests

```bash
dotnet test -c Release
```
