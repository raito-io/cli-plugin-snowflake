// role_name_generator provides role name generation with uniqueness guarantees
// for Snowflake account roles, database roles, and application roles.
//
// It lazily loads existing roles from Snowflake per scope, caches them in memory,
// and generates unique names by appending hex postfixes when collisions occur.
// The generator is safe for concurrent use.
package snowflake

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	importer "github.com/raito-io/cli/base/access_provider/sync_to_target"
	"github.com/raito-io/cli/base/access_provider/sync_to_target/naming_hint"
)

//go:generate go run github.com/vektra/mockery/v2 --name=RoleNameGeneratorRepository --with-expecter --inpackage

// RoleNameGeneratorRepository provides access to existing Snowflake roles.
// It is used to load current role names so the generator can avoid collisions
// and detect renames.
type RoleNameGeneratorRepository interface {
	GetAccountRoles() ([]RoleEntity, error)
	GetDatabaseRoles(database string) ([]RoleEntity, error)
	GetApplicationRoles(application string) ([]ApplicationRoleEntity, error)
}

// RoleNameGenerationResultType indicates the outcome of a role name generation.
type RoleNameGenerationResultType int

const (
	// RoleNameGenerationResultNew indicates a new role name was generated (no prior role existed).
	RoleNameGenerationResultNew RoleNameGenerationResultType = iota
	// RoleNameGenerationResultExisting indicates the role already exists with the correct name; no action needed.
	RoleNameGenerationResultExisting
	// RoleNameGenerationResultRename indicates the role exists but its name has changed and needs to be renamed.
	RoleNameGenerationResultRename

	roleNamePostFixLength = 6
)

// RoleNameGenerator generates unique role names for Snowflake account, database, and application roles.
// It lazily loads existing roles from the repository on first access per scope, caches them,
// and ensures all generated names are unique within their scope by appending hex postfixes when needed.
// It is safe for concurrent use across multiple goroutines.
type RoleNameGenerator struct {
	namingConstraints *naming_hint.NamingConstraints
	translator        naming_hint.Translator
	splitStr          string

	repository RoleNameGeneratorRepository

	// Cache: keyed by scopeEntityString(scope, entity), e.g. "db:MYDB" or ":".
	// Each entry maps base role names to their highest used postfix number.
	roleNameCache map[string]map[string]uint32
	globalMutex   sync.RWMutex
	scopeMutex    map[string]*sync.Mutex
}

// NewRoleNameGenerator creates a new RoleNameGenerator with the given naming constraints and repository.
// The naming constraints control character set, case, max length, and split character for postfixes.
// Returns an error if the naming constraints are invalid (e.g. no allowed characters).
func NewRoleNameGenerator(namingConstraints *naming_hint.NamingConstraints, repository RoleNameGeneratorRepository) (*RoleNameGenerator, error) {
	translator, err := naming_hint.NewNameHintTranslator(namingConstraints)
	if err != nil {
		return nil, fmt.Errorf("create naming hint translator: %w", err)
	}

	return &RoleNameGenerator{
		namingConstraints: namingConstraints,
		translator:        translator,
		splitStr:          fmt.Sprintf("%[1]c%[1]c", namingConstraints.SplitCharacter()),
		repository:        repository,
		roleNameCache:     make(map[string]map[string]uint32),
		scopeMutex:        make(map[string]*sync.Mutex),
	}, nil
}

// GenerateAccountRole generates a unique account-scoped role name for the given access control.
// If the access control has an actual name (HasActualName) and the base name hasn't changed,
// it returns the existing name with RoleNameGenerationResultExisting.
// If the base name changed, it generates a new unique name and returns RoleNameGenerationResultRename.
// For new roles, it returns RoleNameGenerationResultNew.
func (g *RoleNameGenerator) GenerateAccountRole(ap *importer.AccessProvider) (string, RoleNameGenerationResultType, error) {
	return g.generateUniqueRoleName(ap, roleGenScope{"", ""})
}

// GenerateDatabaseRole generates a unique database-scoped role name for the given access control.
// The database parameter specifies which Snowflake database the role belongs to.
// Roles in different databases are tracked independently and cannot collide.
func (g *RoleNameGenerator) GenerateDatabaseRole(ap *importer.AccessProvider, database string) (string, RoleNameGenerationResultType, error) {
	return g.generateUniqueRoleName(ap, roleGenScope{"db", database})
}

// GenerateApplicationRole generates a unique application-scoped role name for the given access control.
// The application parameter specifies which Snowflake application the role belongs to.
// Roles in different applications are tracked independently and cannot collide.
func (g *RoleNameGenerator) GenerateApplicationRole(ap *importer.AccessProvider, application string) (string, RoleNameGenerationResultType, error) {
	return g.generateUniqueRoleName(ap, roleGenScope{"app", application})
}

func (g *RoleNameGenerator) generateUniqueRoleName(ap *importer.AccessProvider, scope roleGenScope) (string, RoleNameGenerationResultType, error) {
	roleName, genType, err := g.generateScopedRoleName(ap, scope)
	if err != nil {
		return "", 0, fmt.Errorf("generate role name: %w", err)
	}

	return roleName, genType, nil
}

// generateScopedRoleName generates a unique role name for the given access control within a scope.
// It acquires the scope-level mutex internally and lazily loads existing roles from the repository.
func (g *RoleNameGenerator) generateScopedRoleName(ap *importer.AccessProvider, scope roleGenScope) (string, RoleNameGenerationResultType, error) {
	mapKey := scope.FullScope()

	// 1. Generate basename
	baseName, err := g.generateBaseName(ap)
	if err != nil {
		return "", 0, fmt.Errorf("generate base name: %w", err)
	}

	resultType := RoleNameGenerationResultNew

	// 2. Check if role name should be updated
	if ap.ActualName != nil {
		originalBaseName, _ := roleNameSplitLast(*ap.ActualName, g.splitStr)

		if originalBaseName == baseName {
			// Existing role and no rename required
			return *ap.ActualName, RoleNameGenerationResultExisting, nil
		}

		// Rename required. Ensure to generate new unique role name
		resultType = RoleNameGenerationResultRename
	}

	mutex := g.getScopeLock(mapKey)

	mutex.Lock()
	defer mutex.Unlock()

	existingRoles, err := g.loadExistingRolesForScope(scope)
	if err != nil {
		return "", 0, fmt.Errorf("load existing roles: %w", err)
	}

	// 3. Generate (new) unique role name
	roleName := g.makeRoleNameUnique(baseName, existingRoles)

	return roleName, resultType, nil
}

func (g *RoleNameGenerator) makeRoleNameUnique(baseName string, existingRoles map[string]uint32) string {
	existingRolePostfix, found := existingRoles[baseName]
	if !found {
		existingRoles[baseName] = 0

		return baseName
	}

	existingRolePostfix++

	postfixId := fmt.Sprintf("%s%X", g.splitStr, existingRolePostfix)

	if !g.namingConstraints.UpperCaseLetters {
		postfixId = strings.ToLower(postfixId)
	}

	existingRoles[baseName] = existingRolePostfix

	return fmt.Sprintf("%s%s", baseName, postfixId)
}

func (g *RoleNameGenerator) generateBaseName(ap *importer.AccessProvider) (string, error) {
	maxLength := g.namingConstraints.MaxLength - roleNamePostFixLength

	var nameHinting string
	if ap.NamingHint != "" {
		nameHinting = ap.NamingHint
	} else {
		nameHinting = ap.Name
	}

	name, err := g.translator.Translate(nameHinting)
	if err != nil {
		return "", fmt.Errorf("translate base name: %w", err)
	}

	if uint(len(name)) > maxLength {
		name = name[:maxLength]
	}

	return name, nil
}

// loadExistingRolesForScope returns the existing roles map for a given scope
// The method is expecting that the scope mutex is already locked.
// The method may return the map from cache (if exists) or load it by reading all roles
func (g *RoleNameGenerator) loadExistingRolesForScope(scope roleGenScope) (map[string]uint32, error) {
	mapKey := scope.FullScope()

	// Check if roles are already populated
	g.globalMutex.RLock()

	if existingRoles, exists := g.roleNameCache[mapKey]; exists {
		g.globalMutex.RUnlock()

		return existingRoles, nil
	}

	g.globalMutex.RUnlock()

	// Roles need to be populated
	g.globalMutex.Lock()
	defer g.globalMutex.Unlock()

	// Double-check after acquiring write lock
	if existingRoles, exists := g.roleNameCache[mapKey]; exists {
		return existingRoles, nil
	}

	var loadRoles func() ([]string, error)

	switch scope.scope {
	case "":
		loadRoles = func() ([]string, error) {
			return roleNameMapArray(g.repository.GetAccountRoles, func(e RoleEntity) string {
				return e.Name
			})
		}
	case "db":
		loadRoles = func() ([]string, error) {
			return roleNameMapArray(func() ([]RoleEntity, error) {
				return g.repository.GetDatabaseRoles(scope.entity)
			}, func(e RoleEntity) string {
				return e.Name
			})
		}
	case "app":
		loadRoles = func() ([]string, error) {
			return roleNameMapArray(func() ([]ApplicationRoleEntity, error) {
				return g.repository.GetApplicationRoles(scope.entity)
			}, func(e ApplicationRoleEntity) string {
				return e.Name
			})
		}
	default:
		return nil, fmt.Errorf("scope %s is not supported", scope)
	}

	roles, err := loadRoles()
	if err != nil {
		return nil, fmt.Errorf("load existing roles: %w", err)
	}

	rolesMap := make(map[string]uint32, len(roles))

	for _, role := range roles {
		baseName, postFix := roleNameSplitLast(role, g.splitStr)

		postFixNumber, err2 := strconv.ParseUint(postFix, 16, 32)
		if err2 != nil {
			// postFix was not a number add an entry of the actual role
			baseName = role
			postFixNumber = 0
		}

		postFixNumber16 := uint32(postFixNumber)

		if value, exists := rolesMap[baseName]; exists {
			rolesMap[baseName] = max(value, postFixNumber16)
		} else {
			rolesMap[baseName] = postFixNumber16
		}
	}

	g.roleNameCache[mapKey] = rolesMap

	return g.roleNameCache[mapKey], nil
}

func (g *RoleNameGenerator) getScopeLock(scope string) *sync.Mutex {
	g.globalMutex.RLock()
	lock, found := g.scopeMutex[scope]
	g.globalMutex.RUnlock()

	if found {
		return lock
	}

	// Mutex not in map
	g.globalMutex.Lock()
	defer g.globalMutex.Unlock()

	lock, found = g.scopeMutex[scope]
	if found {
		return lock
	}

	lock = &sync.Mutex{}
	g.scopeMutex[scope] = lock

	return lock
}

func roleNameMapArray[T any](arrayGenerator func() ([]T, error), fn func(T) string) ([]string, error) {
	originalArray, err := arrayGenerator()
	if err != nil {
		return nil, err
	}

	result := make([]string, len(originalArray))
	for i, original := range originalArray {
		result[i] = fn(original)
	}

	return result, nil
}

func roleNameSplitLast(s, sep string) (string, string) {
	i := strings.LastIndex(s, sep)
	if i == -1 {
		return s, ""
	}
	// s[:i] is the part before the last separator
	// s[i+len(sep):] is the part after the last separator
	return s[:i], s[i+len(sep):]
}

type roleGenScope struct {
	scope  string
	entity string
}

func (r roleGenScope) FullScope() string {
	return fmt.Sprintf("%s:%s", r.scope, r.entity)
}
