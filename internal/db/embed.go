package db

import (
	_ "embed"
)

// BaselineSchema is the full NovoApex schema (extensions excluded — they are
// applied by init-db.sql on first container boot), concatenated in migration
// order from the original Prisma migrations. From the fresh-start decision
// (2026-08-23) this is owned by the Go toolchain: future schema changes append
// idempotent migrations under internal/db/schema/ and bump the version recorded
// in schema_migrations.
//
//go:embed schema/schema.sql
var baselineSchema []byte

func BaselineSchema() []byte {
	return baselineSchema
}
