//go:build testing

package testdata

import "embed"

//go:embed sql/*.sql
var SQLFiles embed.FS
