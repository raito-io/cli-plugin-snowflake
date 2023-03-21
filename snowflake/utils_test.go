package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUtils_CleanDoubleQuotes(t *testing.T) {
	inputExpectedMap := map[string]string{}

	inputExpectedMap[""] = ""
	inputExpectedMap["\"Test User\""] = "Test User"
	inputExpectedMap["\"Test User"] = "\"Test User"
	inputExpectedMap["Test User\""] = "Test User\""
	inputExpectedMap["TEst \"User\" Something"] = "TEst \"User\" Something"
	inputExpectedMap["\""] = "\""
	inputExpectedMap["\"a"] = "\"a"
	inputExpectedMap["b\""] = "b\""
	inputExpectedMap["\"\"Test User\"\""] = "\"Test User\""
	inputExpectedMap["\"AP.WiTh_Dots.And.More-\""] = "AP.WiTh_Dots.And.More-"

	for k, v := range inputExpectedMap {
		assert.Equal(t, v, cleanDoubleQuotes(k))
	}
}
