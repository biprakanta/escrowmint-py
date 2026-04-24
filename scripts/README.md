# Scripts

This directory contains the active Redis Lua sources for the EscrowMint Python mutation contract.

Current contents:

- `try_consume.lua`: exact bounded decrement with optional idempotency
- `reserve.lua`
- `commit.lua`
- `cancel.lua`
- `get_state.lua`

These scripts are the Python runtime source of truth and the intended shared behavioral reference for the Go client as well.
