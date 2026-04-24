# Scripts

This directory contains Redis Lua prototypes for the EscrowMint mutation contract.

Current contents:

- `try_consume.lua`: exact bounded decrement with optional idempotency

Planned:

- `reserve.lua`
- `commit.lua`
- `cancel.lua`

These scripts are intended as the shared behavioral core for both Python and Go clients.
