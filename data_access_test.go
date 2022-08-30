package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateRoleName(t *testing.T) {
	assert.Equal(t, "HELLO", generateRoleNameFromAPName("hello"))
	assert.Equal(t, "HE_LLO", generateRoleNameFromAPName("He_llo"))
	assert.Equal(t, "THIS_IS_OK", generateRoleNameFromAPName("This is ok?"))
}
