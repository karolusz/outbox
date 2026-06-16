//go:build testing

package relay

import "embed"

// sqlFixtures embeds the SQL seed scripts under testdata/sql/. They are
// consumed by setupTest via testutils.SeedTables. The conventional
// "testdata" directory name keeps these out of the regular package
// build; the //go:embed directive is the explicit way back in.
//
//go:embed testdata/sql/*.sql
var sqlFixtures embed.FS
