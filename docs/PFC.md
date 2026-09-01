# PFC (Piecewise Fountain Code)

PFC is the Wirehair fountain path on **wire v4** (`--codec wirehair`).
It is not compatible with v3 codecs (`copy`, `xor-fec`, `rs`, `rs-fec`).

| Document | Audience | Contents |
| --- | --- | --- |
| [PFC_INTEGRATION.md](PFC_INTEGRATION.md) | Integrators | How to build encoder, relay, and decoder (library, binaries, modules) |
| [PFC_EXPERIMENTS.md](PFC_EXPERIMENTS.md) | Operators and experiment runners | Build, flags, topologies, metrics, VM matrix |
| [PFC_ARCHITECTURE.md](PFC_ARCHITECTURE.md) | Developers | Layers, packet path, functions, library embedding |

Codec library: `third_party/wirehair`.  
CLI name: `--codec wirehair`.
