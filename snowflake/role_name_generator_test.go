package snowflake

import (
	"sync"
	"testing"

	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target/naming_hint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// roleGenTestConstraints mirrors the production RoleNameConstraints from data_source_meta_data.go.
// With these constraints the translator uppercases names and replaces invalid chars with "_".
// SplitCharacter() returns '_', so splitStr becomes "__".
var roleGenTestConstraints = &naming_hint.NamingConstraints{
	UpperCaseLetters:  true,
	LowerCaseLetters:  false,
	Numbers:           true,
	SpecialCharacters: "_$",
	MaxLength:         255,
}

// buildRoleGenAP creates an AccessControlToTarget for testing.
// Pass empty string for namingHint or actualName to leave them unset.
func buildRoleGenAP(name, namingHint, actualName string) *importer.AccessProvider {
	ap := &importer.AccessProvider{
		Name:       name,
		NamingHint: namingHint,
	}

	if actualName != "" {
		ap.ActualName = &actualName
	}

	return ap

}

// --- New role creation flow ---

func TestGenerateAccountRole_NewRole_NoExistingRoles(t *testing.T) {
	// Given: no existing roles in Snowflake
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{}, nil).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	// The translator converts camelCase to UPPER_CASE (inserts _ at word boundaries)
	ap := buildRoleGenAP("MyRole", "", "")

	// When: generating a new account role
	roleName, resultType, err := gen.GenerateAccountRole(ap)

	// Then: name is translated (uppercased with word separators), result is New
	require.NoError(t, err)
	assert.Equal(t, "MY_ROLE", roleName)
	assert.Equal(t, RoleNameGenerationResultNew, resultType)
}

func TestGenerateAccountRole_NewRole_WithPostfix(t *testing.T) {
	// Given: "MY_ROLE" already exists in Snowflake
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{
		{Name: "MY_ROLE"},
	}, nil).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	// When: generating first role with same base name
	roleName1, resultType1, err := gen.GenerateAccountRole(buildRoleGenAP("MyRole", "", ""))

	// Then: postfix __1 appended
	require.NoError(t, err)
	assert.Equal(t, "MY_ROLE__1", roleName1)
	assert.Equal(t, RoleNameGenerationResultNew, resultType1)

	// When: generating second role with same base name
	roleName2, resultType2, err := gen.GenerateAccountRole(buildRoleGenAP("MyRole", "", ""))

	// Then: postfix __2 appended, repository was only called once (cache reused)
	require.NoError(t, err)
	assert.Equal(t, "MY_ROLE__2", roleName2)
	assert.Equal(t, RoleNameGenerationResultNew, resultType2)
}

func TestGenerateAccountRole_NewRole_ContinuesFromHighestPostfix(t *testing.T) {
	// Given: roles with gaps in postfix numbering exist
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{
		{Name: "ROLE"},
		{Name: "ROLE__3"},
	}, nil).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	ap := buildRoleGenAP("Role", "", "")

	// When: generating a new role with the same base name
	roleName, resultType, err := gen.GenerateAccountRole(ap)

	// Then: continues from highest existing postfix (3) -> __4
	require.NoError(t, err)
	assert.Equal(t, "ROLE__4", roleName)
	assert.Equal(t, RoleNameGenerationResultNew, resultType)
}

func TestGenerateAccountRole_NamingHintTakesPrecedence(t *testing.T) {
	// Given: no existing roles
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{}, nil).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	// When: naming hint differs from the name
	ap := buildRoleGenAP("OriginalName", "HINTED", "")
	roleName, _, err := gen.GenerateAccountRole(ap)

	// Then: naming hint is used, not the name
	require.NoError(t, err)
	assert.Equal(t, "HINTED", roleName)
}

func TestGenerateAccountRole_NameTruncatedToMaxLength(t *testing.T) {
	// Given: constraints with small max length (15 - 6 postfix = 9 chars for base)
	shortConstraints := &naming_hint.NamingConstraints{
		UpperCaseLetters:  true,
		LowerCaseLetters:  false,
		Numbers:           true,
		SpecialCharacters: "_$",
		MaxLength:         15,
	}

	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{}, nil).Once()

	gen, err := NewRoleNameGenerator(shortConstraints, repoMock)
	require.NoError(t, err)

	// When: translated name exceeds the max base length (9 chars)
	// "VERYLONGROLENAME" -> "VERYLONGROLENAME" (16 chars, already uppercase) -> truncated to 9
	ap := buildRoleGenAP("VERYLONGROLENAME", "", "")
	roleName, _, err := gen.GenerateAccountRole(ap)

	// Then: name is truncated to 9 characters (maxLength 15 - postFixLength 6)
	require.NoError(t, err)
	assert.Len(t, roleName, 9)
	assert.Equal(t, "VERYLONGR", roleName)
}

// --- Existing/rename flow ---

func TestGenerateAccountRole_ExistingRole_NoRenameNeeded(t *testing.T) {
	// Given: the access control has actualName "MY_ROLE" matching the translated base name
	repoMock := NewMockRoleNameGeneratorRepository(t)

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	// "MyRole" translates to "MY_ROLE", matching the actualName
	ap := buildRoleGenAP("MyRole", "", "MY_ROLE")

	// When: generating for an existing role whose base name hasn't changed
	roleName, resultType, err := gen.GenerateAccountRole(ap)

	// Then: returns the existing name unchanged, without loading roles from repo
	require.NoError(t, err)
	assert.Equal(t, "MY_ROLE", roleName)
	assert.Equal(t, RoleNameGenerationResultExisting, resultType)
}

func TestGenerateAccountRole_ExistingRole_WithPostfix_NoRenameNeeded(t *testing.T) {
	// Given: the access control has actualName "READER__2" whose base matches "READER"
	repoMock := NewMockRoleNameGeneratorRepository(t)

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	// "READER" translates to "READER", matching base name of actualName "READER__2"
	ap := buildRoleGenAP("READER", "", "READER__2")

	// When: the base name still matches (READER == READER)
	roleName, resultType, err := gen.GenerateAccountRole(ap)

	// Then: returns the actual name unchanged, including its postfix
	require.NoError(t, err)
	assert.Equal(t, "READER__2", roleName)
	assert.Equal(t, RoleNameGenerationResultExisting, resultType)
}

func TestGenerateAccountRole_ExistingRole_RenameRequired(t *testing.T) {
	// Given: role "OLDROLE" exists but the desired name is now "NEWROLE"
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{
		{Name: "OLDROLE"},
	}, nil).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	ap := buildRoleGenAP("NEWROLE", "", "OLDROLE")

	// When: base name changed from OLDROLE to NEWROLE
	roleName, resultType, err := gen.GenerateAccountRole(ap)

	// Then: generates new name, result is Rename
	require.NoError(t, err)
	assert.Equal(t, "NEWROLE", roleName)
	assert.Equal(t, RoleNameGenerationResultRename, resultType)
}

func TestGenerateAccountRole_ExistingRole_RenameWithConflict(t *testing.T) {
	// Given: "NEWROLE" already exists and "OLDROLE" needs to be renamed to "NEWROLE"
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{
		{Name: "NEWROLE"},
		{Name: "OLDROLE"},
	}, nil).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	ap := buildRoleGenAP("NEWROLE", "", "OLDROLE")

	// When: rename target conflicts with an existing role
	roleName, resultType, err := gen.GenerateAccountRole(ap)

	// Then: appends postfix to make the rename unique
	require.NoError(t, err)
	assert.Equal(t, "NEWROLE__1", roleName)
	assert.Equal(t, RoleNameGenerationResultRename, resultType)
}

// --- Scope-specific tests ---

func TestGenerateDatabaseRole_NewRole(t *testing.T) {
	// Given: no existing database roles in MYDB
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetDatabaseRoles("MYDB").Return([]RoleEntity{}, nil).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	ap := buildRoleGenAP("READER", "", "")

	// When: generating a database role
	roleName, resultType, err := gen.GenerateDatabaseRole(ap, "MYDB")

	// Then: generates a new role name scoped to the database
	require.NoError(t, err)
	assert.Equal(t, "READER", roleName)
	assert.Equal(t, RoleNameGenerationResultNew, resultType)
}

func TestGenerateApplicationRole_NewRole(t *testing.T) {
	// Given: no existing application roles in MYAPP
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetApplicationRoles("MYAPP").Return([]ApplicationRoleEntity{}, nil).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	ap := buildRoleGenAP("READER", "", "")

	// When: generating an application role
	roleName, resultType, err := gen.GenerateApplicationRole(ap, "MYAPP")

	// Then: generates a new role name scoped to the application
	require.NoError(t, err)
	assert.Equal(t, "READER", roleName)
	assert.Equal(t, RoleNameGenerationResultNew, resultType)
}

func TestDifferentScopes_IndependentCaches(t *testing.T) {
	// Given: account roles and database roles both contain "READER"
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{
		{Name: "READER"},
	}, nil).Once()
	repoMock.EXPECT().GetDatabaseRoles("MYDB").Return([]RoleEntity{
		{Name: "READER"},
	}, nil).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	// When: generating in account scope (conflict -> postfix)
	accountName, _, err := gen.GenerateAccountRole(buildRoleGenAP("READER", "", ""))
	require.NoError(t, err)

	// When: generating in database scope (independent conflict -> same postfix)
	dbName, _, err := gen.GenerateDatabaseRole(buildRoleGenAP("READER", "", ""), "MYDB")
	require.NoError(t, err)

	// Then: both get __1 because scopes are independent
	assert.Equal(t, "READER__1", accountName)
	assert.Equal(t, "READER__1", dbName)
}

// --- Error cases ---

func TestGenerateAccountRole_RepositoryError(t *testing.T) {
	// Given: repository returns an error
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetAccountRoles().Return(nil, assert.AnError).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	ap := buildRoleGenAP("ROLE", "", "")

	// When: generating a role
	_, _, err = gen.GenerateAccountRole(ap)

	// Then: error is propagated
	require.Error(t, err)
	assert.ErrorContains(t, err, "load existing roles")
}

func TestGenerateDatabaseRole_RepositoryError(t *testing.T) {
	// Given: repository returns an error for database roles
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetDatabaseRoles("MYDB").Return(nil, assert.AnError).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	ap := buildRoleGenAP("ROLE", "", "")

	// When: generating a database role
	_, _, err = gen.GenerateDatabaseRole(ap, "MYDB")

	// Then: error is propagated
	require.Error(t, err)
	assert.ErrorContains(t, err, "load existing roles")
}

// --- Cache reuse ---

func TestGenerateAccountRole_CacheIsReused(t *testing.T) {
	// Given: repository returns roles
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{}, nil).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	// When: generating two different roles
	name1, _, err := gen.GenerateAccountRole(buildRoleGenAP("ALPHA", "", ""))
	require.NoError(t, err)

	name2, _, err := gen.GenerateAccountRole(buildRoleGenAP("BETA", "", ""))
	require.NoError(t, err)

	// Then: both succeed and repository was called only once (.Once() enforces this)
	assert.Equal(t, "ALPHA", name1)
	assert.Equal(t, "BETA", name2)
}

// --- Thread-safety ---

func TestGenerateAccountRole_ConcurrentAccess(t *testing.T) {
	// Given: repository returns an empty role list
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{}, nil).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	const goroutines = 50

	// When: many goroutines generate roles with the same base name concurrently
	var wg sync.WaitGroup

	results := make([]string, goroutines)

	wg.Add(goroutines)

	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()

			name, resultType, genErr := gen.GenerateAccountRole(buildRoleGenAP("SHARED", "", ""))
			assert.NoError(t, genErr)
			assert.Equal(t, RoleNameGenerationResultNew, resultType)

			results[idx] = name
		}(i)
	}

	wg.Wait()

	// Then: all generated names are unique
	seen := make(map[string]bool, goroutines)

	for _, name := range results {
		assert.False(t, seen[name], "duplicate role name generated: %s", name)
		seen[name] = true
	}

	assert.Len(t, seen, goroutines)
}

func TestGenerateRoles_ConcurrentDifferentScopes(t *testing.T) {
	// Given: repository returns empty lists for both scopes
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{}, nil).Once()
	repoMock.EXPECT().GetDatabaseRoles("DB1").Return([]RoleEntity{}, nil).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	const goroutines = 20

	// When: goroutines generate roles in different scopes concurrently
	var wg sync.WaitGroup

	wg.Add(goroutines * 2)

	for range goroutines {
		go func() {
			defer wg.Done()

			_, _, genErr := gen.GenerateAccountRole(buildRoleGenAP("ROLE", "", ""))
			assert.NoError(t, genErr)
		}()

		go func() {
			defer wg.Done()

			_, _, genErr := gen.GenerateDatabaseRole(buildRoleGenAP("ROLE", "", ""), "DB1")
			assert.NoError(t, genErr)
		}()
	}

	wg.Wait()
}

// --- Mixed scenario ---

func TestGenerateAccountRole_MixedScenario(t *testing.T) {
	// Given: some roles already exist in Snowflake
	repoMock := NewMockRoleNameGeneratorRepository(t)
	repoMock.EXPECT().GetAccountRoles().Return([]RoleEntity{
		{Name: "ALPHA"},
		{Name: "BETA"},
		{Name: "BETA__1"},
	}, nil).Once()

	gen, err := NewRoleNameGenerator(roleGenTestConstraints, repoMock)
	require.NoError(t, err)

	// When/Then: existing role "ALPHA" with no rename needed
	name, resultType, err := gen.GenerateAccountRole(buildRoleGenAP("Alpha", "", "ALPHA"))
	require.NoError(t, err)
	assert.Equal(t, "ALPHA", name)
	assert.Equal(t, RoleNameGenerationResultExisting, resultType)

	// When/Then: new role "GAMMA" with no conflicts
	name, resultType, err = gen.GenerateAccountRole(buildRoleGenAP("Gamma", "", ""))
	require.NoError(t, err)
	assert.Equal(t, "GAMMA", name)
	assert.Equal(t, RoleNameGenerationResultNew, resultType)

	// When/Then: new role "BETA" conflicts with existing -> gets postfix __2
	name, resultType, err = gen.GenerateAccountRole(buildRoleGenAP("Beta", "", ""))
	require.NoError(t, err)
	assert.Equal(t, "BETA__2", name)
	assert.Equal(t, RoleNameGenerationResultNew, resultType)

	// When/Then: rename from ALPHA to BETA -> conflicts -> gets postfix __3
	name, resultType, err = gen.GenerateAccountRole(buildRoleGenAP("Beta", "", "ALPHA"))
	require.NoError(t, err)
	assert.Equal(t, "BETA__3", name)
	assert.Equal(t, RoleNameGenerationResultRename, resultType)
}

// --- Helper function tests ---

func TestRoleNameSplitLast(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		sep           string
		expectedLeft  string
		expectedRight string
	}{
		{
			name:          "standard split",
			input:         "ROLE__3",
			sep:           "__",
			expectedLeft:  "ROLE",
			expectedRight: "3",
		},
		{
			name:          "no separator found",
			input:         "ROLE",
			sep:           "__",
			expectedLeft:  "ROLE",
			expectedRight: "",
		},
		{
			name:          "splits on last occurrence",
			input:         "A__B__C",
			sep:           "__",
			expectedLeft:  "A__B",
			expectedRight: "C",
		},
		{
			name:          "empty string",
			input:         "",
			sep:           "__",
			expectedLeft:  "",
			expectedRight: "",
		},
		{
			name:          "separator only",
			input:         "__",
			sep:           "__",
			expectedLeft:  "",
			expectedRight: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left, right := roleNameSplitLast(tt.input, tt.sep)

			assert.Equal(t, tt.expectedLeft, left)
			assert.Equal(t, tt.expectedRight, right)
		})
	}
}

func TestRoleGenScope_FullScope(t *testing.T) {
	tests := []struct {
		name     string
		scope    string
		entity   string
		expected string
	}{
		{
			name:     "account scope",
			scope:    "",
			entity:   "",
			expected: ":",
		},
		{
			name:     "database scope",
			scope:    "db",
			entity:   "MYDB",
			expected: "db:MYDB",
		},
		{
			name:     "application scope",
			scope:    "app",
			entity:   "MYAPP",
			expected: "app:MYAPP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := roleGenScope{tt.scope, tt.entity}.FullScope()

			assert.Equal(t, tt.expected, result)
		})
	}
}
