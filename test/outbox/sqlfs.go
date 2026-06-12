//go:build testing

package integrationoutbox

import "embed"

//go:embed testdata/sql/*.sql
var SQLFiles embed.FS
