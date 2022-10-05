package snowflake

import (
	"github.com/hashicorp/go-hclog"
	"github.com/raito-io/cli/base"
)

var logger hclog.Logger

func init() {
	logger = base.Logger()
}
