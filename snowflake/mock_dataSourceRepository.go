// Code generated by mockery v2.51.0. DO NOT EDIT.

package snowflake

import (
	tag "github.com/raito-io/cli/base/tag"
	mock "github.com/stretchr/testify/mock"

	time "time"
)

// mockDataSourceRepository is an autogenerated mock type for the dataSourceRepository type
type mockDataSourceRepository struct {
	mock.Mock
}

type mockDataSourceRepository_Expecter struct {
	mock *mock.Mock
}

func (_m *mockDataSourceRepository) EXPECT() *mockDataSourceRepository_Expecter {
	return &mockDataSourceRepository_Expecter{mock: &_m.Mock}
}

// Close provides a mock function with no fields
func (_m *mockDataSourceRepository) Close() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Close")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// mockDataSourceRepository_Close_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Close'
type mockDataSourceRepository_Close_Call struct {
	*mock.Call
}

// Close is a helper method to define mock.On call
func (_e *mockDataSourceRepository_Expecter) Close() *mockDataSourceRepository_Close_Call {
	return &mockDataSourceRepository_Close_Call{Call: _e.mock.On("Close")}
}

func (_c *mockDataSourceRepository_Close_Call) Run(run func()) *mockDataSourceRepository_Close_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockDataSourceRepository_Close_Call) Return(_a0 error) *mockDataSourceRepository_Close_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceRepository_Close_Call) RunAndReturn(run func() error) *mockDataSourceRepository_Close_Call {
	_c.Call.Return(run)
	return _c
}

// ExecuteGrantOnAccountRole provides a mock function with given fields: perm, on, role, isSystemGrant
func (_m *mockDataSourceRepository) ExecuteGrantOnAccountRole(perm string, on string, role string, isSystemGrant bool) error {
	ret := _m.Called(perm, on, role, isSystemGrant)

	if len(ret) == 0 {
		panic("no return value specified for ExecuteGrantOnAccountRole")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, string, bool) error); ok {
		r0 = rf(perm, on, role, isSystemGrant)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// mockDataSourceRepository_ExecuteGrantOnAccountRole_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ExecuteGrantOnAccountRole'
type mockDataSourceRepository_ExecuteGrantOnAccountRole_Call struct {
	*mock.Call
}

// ExecuteGrantOnAccountRole is a helper method to define mock.On call
//   - perm string
//   - on string
//   - role string
//   - isSystemGrant bool
func (_e *mockDataSourceRepository_Expecter) ExecuteGrantOnAccountRole(perm interface{}, on interface{}, role interface{}, isSystemGrant interface{}) *mockDataSourceRepository_ExecuteGrantOnAccountRole_Call {
	return &mockDataSourceRepository_ExecuteGrantOnAccountRole_Call{Call: _e.mock.On("ExecuteGrantOnAccountRole", perm, on, role, isSystemGrant)}
}

func (_c *mockDataSourceRepository_ExecuteGrantOnAccountRole_Call) Run(run func(perm string, on string, role string, isSystemGrant bool)) *mockDataSourceRepository_ExecuteGrantOnAccountRole_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(string), args[3].(bool))
	})
	return _c
}

func (_c *mockDataSourceRepository_ExecuteGrantOnAccountRole_Call) Return(_a0 error) *mockDataSourceRepository_ExecuteGrantOnAccountRole_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceRepository_ExecuteGrantOnAccountRole_Call) RunAndReturn(run func(string, string, string, bool) error) *mockDataSourceRepository_ExecuteGrantOnAccountRole_Call {
	_c.Call.Return(run)
	return _c
}

// GetColumnsInDatabase provides a mock function with given fields: databaseName, handleEntity
func (_m *mockDataSourceRepository) GetColumnsInDatabase(databaseName string, handleEntity EntityHandler) error {
	ret := _m.Called(databaseName, handleEntity)

	if len(ret) == 0 {
		panic("no return value specified for GetColumnsInDatabase")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, EntityHandler) error); ok {
		r0 = rf(databaseName, handleEntity)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// mockDataSourceRepository_GetColumnsInDatabase_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetColumnsInDatabase'
type mockDataSourceRepository_GetColumnsInDatabase_Call struct {
	*mock.Call
}

// GetColumnsInDatabase is a helper method to define mock.On call
//   - databaseName string
//   - handleEntity EntityHandler
func (_e *mockDataSourceRepository_Expecter) GetColumnsInDatabase(databaseName interface{}, handleEntity interface{}) *mockDataSourceRepository_GetColumnsInDatabase_Call {
	return &mockDataSourceRepository_GetColumnsInDatabase_Call{Call: _e.mock.On("GetColumnsInDatabase", databaseName, handleEntity)}
}

func (_c *mockDataSourceRepository_GetColumnsInDatabase_Call) Run(run func(databaseName string, handleEntity EntityHandler)) *mockDataSourceRepository_GetColumnsInDatabase_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(EntityHandler))
	})
	return _c
}

func (_c *mockDataSourceRepository_GetColumnsInDatabase_Call) Return(_a0 error) *mockDataSourceRepository_GetColumnsInDatabase_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceRepository_GetColumnsInDatabase_Call) RunAndReturn(run func(string, EntityHandler) error) *mockDataSourceRepository_GetColumnsInDatabase_Call {
	_c.Call.Return(run)
	return _c
}

// GetDatabases provides a mock function with no fields
func (_m *mockDataSourceRepository) GetDatabases() ([]DbEntity, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetDatabases")
	}

	var r0 []DbEntity
	var r1 error
	if rf, ok := ret.Get(0).(func() ([]DbEntity, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() []DbEntity); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]DbEntity)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataSourceRepository_GetDatabases_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetDatabases'
type mockDataSourceRepository_GetDatabases_Call struct {
	*mock.Call
}

// GetDatabases is a helper method to define mock.On call
func (_e *mockDataSourceRepository_Expecter) GetDatabases() *mockDataSourceRepository_GetDatabases_Call {
	return &mockDataSourceRepository_GetDatabases_Call{Call: _e.mock.On("GetDatabases")}
}

func (_c *mockDataSourceRepository_GetDatabases_Call) Run(run func()) *mockDataSourceRepository_GetDatabases_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockDataSourceRepository_GetDatabases_Call) Return(_a0 []DbEntity, _a1 error) *mockDataSourceRepository_GetDatabases_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceRepository_GetDatabases_Call) RunAndReturn(run func() ([]DbEntity, error)) *mockDataSourceRepository_GetDatabases_Call {
	_c.Call.Return(run)
	return _c
}

// GetFunctionsInDatabase provides a mock function with given fields: databaseName, handleEntity
func (_m *mockDataSourceRepository) GetFunctionsInDatabase(databaseName string, handleEntity EntityHandler) error {
	ret := _m.Called(databaseName, handleEntity)

	if len(ret) == 0 {
		panic("no return value specified for GetFunctionsInDatabase")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, EntityHandler) error); ok {
		r0 = rf(databaseName, handleEntity)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// mockDataSourceRepository_GetFunctionsInDatabase_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetFunctionsInDatabase'
type mockDataSourceRepository_GetFunctionsInDatabase_Call struct {
	*mock.Call
}

// GetFunctionsInDatabase is a helper method to define mock.On call
//   - databaseName string
//   - handleEntity EntityHandler
func (_e *mockDataSourceRepository_Expecter) GetFunctionsInDatabase(databaseName interface{}, handleEntity interface{}) *mockDataSourceRepository_GetFunctionsInDatabase_Call {
	return &mockDataSourceRepository_GetFunctionsInDatabase_Call{Call: _e.mock.On("GetFunctionsInDatabase", databaseName, handleEntity)}
}

func (_c *mockDataSourceRepository_GetFunctionsInDatabase_Call) Run(run func(databaseName string, handleEntity EntityHandler)) *mockDataSourceRepository_GetFunctionsInDatabase_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(EntityHandler))
	})
	return _c
}

func (_c *mockDataSourceRepository_GetFunctionsInDatabase_Call) Return(_a0 error) *mockDataSourceRepository_GetFunctionsInDatabase_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceRepository_GetFunctionsInDatabase_Call) RunAndReturn(run func(string, EntityHandler) error) *mockDataSourceRepository_GetFunctionsInDatabase_Call {
	_c.Call.Return(run)
	return _c
}

// GetInboundShares provides a mock function with no fields
func (_m *mockDataSourceRepository) GetInboundShares() ([]DbEntity, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetInboundShares")
	}

	var r0 []DbEntity
	var r1 error
	if rf, ok := ret.Get(0).(func() ([]DbEntity, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() []DbEntity); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]DbEntity)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataSourceRepository_GetInboundShares_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetInboundShares'
type mockDataSourceRepository_GetInboundShares_Call struct {
	*mock.Call
}

// GetInboundShares is a helper method to define mock.On call
func (_e *mockDataSourceRepository_Expecter) GetInboundShares() *mockDataSourceRepository_GetInboundShares_Call {
	return &mockDataSourceRepository_GetInboundShares_Call{Call: _e.mock.On("GetInboundShares")}
}

func (_c *mockDataSourceRepository_GetInboundShares_Call) Run(run func()) *mockDataSourceRepository_GetInboundShares_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockDataSourceRepository_GetInboundShares_Call) Return(_a0 []DbEntity, _a1 error) *mockDataSourceRepository_GetInboundShares_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceRepository_GetInboundShares_Call) RunAndReturn(run func() ([]DbEntity, error)) *mockDataSourceRepository_GetInboundShares_Call {
	_c.Call.Return(run)
	return _c
}

// GetIntegrations provides a mock function with no fields
func (_m *mockDataSourceRepository) GetIntegrations() ([]DbEntity, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetIntegrations")
	}

	var r0 []DbEntity
	var r1 error
	if rf, ok := ret.Get(0).(func() ([]DbEntity, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() []DbEntity); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]DbEntity)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataSourceRepository_GetIntegrations_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetIntegrations'
type mockDataSourceRepository_GetIntegrations_Call struct {
	*mock.Call
}

// GetIntegrations is a helper method to define mock.On call
func (_e *mockDataSourceRepository_Expecter) GetIntegrations() *mockDataSourceRepository_GetIntegrations_Call {
	return &mockDataSourceRepository_GetIntegrations_Call{Call: _e.mock.On("GetIntegrations")}
}

func (_c *mockDataSourceRepository_GetIntegrations_Call) Run(run func()) *mockDataSourceRepository_GetIntegrations_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockDataSourceRepository_GetIntegrations_Call) Return(_a0 []DbEntity, _a1 error) *mockDataSourceRepository_GetIntegrations_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceRepository_GetIntegrations_Call) RunAndReturn(run func() ([]DbEntity, error)) *mockDataSourceRepository_GetIntegrations_Call {
	_c.Call.Return(run)
	return _c
}

// GetProceduresInDatabase provides a mock function with given fields: databaseName, handleEntity
func (_m *mockDataSourceRepository) GetProceduresInDatabase(databaseName string, handleEntity EntityHandler) error {
	ret := _m.Called(databaseName, handleEntity)

	if len(ret) == 0 {
		panic("no return value specified for GetProceduresInDatabase")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, EntityHandler) error); ok {
		r0 = rf(databaseName, handleEntity)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// mockDataSourceRepository_GetProceduresInDatabase_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetProceduresInDatabase'
type mockDataSourceRepository_GetProceduresInDatabase_Call struct {
	*mock.Call
}

// GetProceduresInDatabase is a helper method to define mock.On call
//   - databaseName string
//   - handleEntity EntityHandler
func (_e *mockDataSourceRepository_Expecter) GetProceduresInDatabase(databaseName interface{}, handleEntity interface{}) *mockDataSourceRepository_GetProceduresInDatabase_Call {
	return &mockDataSourceRepository_GetProceduresInDatabase_Call{Call: _e.mock.On("GetProceduresInDatabase", databaseName, handleEntity)}
}

func (_c *mockDataSourceRepository_GetProceduresInDatabase_Call) Run(run func(databaseName string, handleEntity EntityHandler)) *mockDataSourceRepository_GetProceduresInDatabase_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(EntityHandler))
	})
	return _c
}

func (_c *mockDataSourceRepository_GetProceduresInDatabase_Call) Return(_a0 error) *mockDataSourceRepository_GetProceduresInDatabase_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceRepository_GetProceduresInDatabase_Call) RunAndReturn(run func(string, EntityHandler) error) *mockDataSourceRepository_GetProceduresInDatabase_Call {
	_c.Call.Return(run)
	return _c
}

// GetSchemasInDatabase provides a mock function with given fields: databaseName, handleEntity
func (_m *mockDataSourceRepository) GetSchemasInDatabase(databaseName string, handleEntity EntityHandler) error {
	ret := _m.Called(databaseName, handleEntity)

	if len(ret) == 0 {
		panic("no return value specified for GetSchemasInDatabase")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, EntityHandler) error); ok {
		r0 = rf(databaseName, handleEntity)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// mockDataSourceRepository_GetSchemasInDatabase_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSchemasInDatabase'
type mockDataSourceRepository_GetSchemasInDatabase_Call struct {
	*mock.Call
}

// GetSchemasInDatabase is a helper method to define mock.On call
//   - databaseName string
//   - handleEntity EntityHandler
func (_e *mockDataSourceRepository_Expecter) GetSchemasInDatabase(databaseName interface{}, handleEntity interface{}) *mockDataSourceRepository_GetSchemasInDatabase_Call {
	return &mockDataSourceRepository_GetSchemasInDatabase_Call{Call: _e.mock.On("GetSchemasInDatabase", databaseName, handleEntity)}
}

func (_c *mockDataSourceRepository_GetSchemasInDatabase_Call) Run(run func(databaseName string, handleEntity EntityHandler)) *mockDataSourceRepository_GetSchemasInDatabase_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(EntityHandler))
	})
	return _c
}

func (_c *mockDataSourceRepository_GetSchemasInDatabase_Call) Return(_a0 error) *mockDataSourceRepository_GetSchemasInDatabase_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceRepository_GetSchemasInDatabase_Call) RunAndReturn(run func(string, EntityHandler) error) *mockDataSourceRepository_GetSchemasInDatabase_Call {
	_c.Call.Return(run)
	return _c
}

// GetSnowFlakeAccountName provides a mock function with given fields: ops
func (_m *mockDataSourceRepository) GetSnowFlakeAccountName(ops ...func(*GetSnowFlakeAccountNameOptions)) (string, error) {
	_va := make([]interface{}, len(ops))
	for _i := range ops {
		_va[_i] = ops[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for GetSnowFlakeAccountName")
	}

	var r0 string
	var r1 error
	if rf, ok := ret.Get(0).(func(...func(*GetSnowFlakeAccountNameOptions)) (string, error)); ok {
		return rf(ops...)
	}
	if rf, ok := ret.Get(0).(func(...func(*GetSnowFlakeAccountNameOptions)) string); ok {
		r0 = rf(ops...)
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func(...func(*GetSnowFlakeAccountNameOptions)) error); ok {
		r1 = rf(ops...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataSourceRepository_GetSnowFlakeAccountName_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSnowFlakeAccountName'
type mockDataSourceRepository_GetSnowFlakeAccountName_Call struct {
	*mock.Call
}

// GetSnowFlakeAccountName is a helper method to define mock.On call
//   - ops ...func(*GetSnowFlakeAccountNameOptions)
func (_e *mockDataSourceRepository_Expecter) GetSnowFlakeAccountName(ops ...interface{}) *mockDataSourceRepository_GetSnowFlakeAccountName_Call {
	return &mockDataSourceRepository_GetSnowFlakeAccountName_Call{Call: _e.mock.On("GetSnowFlakeAccountName",
		append([]interface{}{}, ops...)...)}
}

func (_c *mockDataSourceRepository_GetSnowFlakeAccountName_Call) Run(run func(ops ...func(*GetSnowFlakeAccountNameOptions))) *mockDataSourceRepository_GetSnowFlakeAccountName_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]func(*GetSnowFlakeAccountNameOptions), len(args)-0)
		for i, a := range args[0:] {
			if a != nil {
				variadicArgs[i] = a.(func(*GetSnowFlakeAccountNameOptions))
			}
		}
		run(variadicArgs...)
	})
	return _c
}

func (_c *mockDataSourceRepository_GetSnowFlakeAccountName_Call) Return(_a0 string, _a1 error) *mockDataSourceRepository_GetSnowFlakeAccountName_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceRepository_GetSnowFlakeAccountName_Call) RunAndReturn(run func(...func(*GetSnowFlakeAccountNameOptions)) (string, error)) *mockDataSourceRepository_GetSnowFlakeAccountName_Call {
	_c.Call.Return(run)
	return _c
}

// GetTablesInDatabase provides a mock function with given fields: databaseName, schemaName, handleEntity
func (_m *mockDataSourceRepository) GetTablesInDatabase(databaseName string, schemaName string, handleEntity EntityHandler) error {
	ret := _m.Called(databaseName, schemaName, handleEntity)

	if len(ret) == 0 {
		panic("no return value specified for GetTablesInDatabase")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, EntityHandler) error); ok {
		r0 = rf(databaseName, schemaName, handleEntity)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// mockDataSourceRepository_GetTablesInDatabase_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetTablesInDatabase'
type mockDataSourceRepository_GetTablesInDatabase_Call struct {
	*mock.Call
}

// GetTablesInDatabase is a helper method to define mock.On call
//   - databaseName string
//   - schemaName string
//   - handleEntity EntityHandler
func (_e *mockDataSourceRepository_Expecter) GetTablesInDatabase(databaseName interface{}, schemaName interface{}, handleEntity interface{}) *mockDataSourceRepository_GetTablesInDatabase_Call {
	return &mockDataSourceRepository_GetTablesInDatabase_Call{Call: _e.mock.On("GetTablesInDatabase", databaseName, schemaName, handleEntity)}
}

func (_c *mockDataSourceRepository_GetTablesInDatabase_Call) Run(run func(databaseName string, schemaName string, handleEntity EntityHandler)) *mockDataSourceRepository_GetTablesInDatabase_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(EntityHandler))
	})
	return _c
}

func (_c *mockDataSourceRepository_GetTablesInDatabase_Call) Return(_a0 error) *mockDataSourceRepository_GetTablesInDatabase_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceRepository_GetTablesInDatabase_Call) RunAndReturn(run func(string, string, EntityHandler) error) *mockDataSourceRepository_GetTablesInDatabase_Call {
	_c.Call.Return(run)
	return _c
}

// GetTagsByDomain provides a mock function with given fields: domain
func (_m *mockDataSourceRepository) GetTagsByDomain(domain string) (map[string][]*tag.Tag, error) {
	ret := _m.Called(domain)

	if len(ret) == 0 {
		panic("no return value specified for GetTagsByDomain")
	}

	var r0 map[string][]*tag.Tag
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (map[string][]*tag.Tag, error)); ok {
		return rf(domain)
	}
	if rf, ok := ret.Get(0).(func(string) map[string][]*tag.Tag); ok {
		r0 = rf(domain)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string][]*tag.Tag)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(domain)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataSourceRepository_GetTagsByDomain_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetTagsByDomain'
type mockDataSourceRepository_GetTagsByDomain_Call struct {
	*mock.Call
}

// GetTagsByDomain is a helper method to define mock.On call
//   - domain string
func (_e *mockDataSourceRepository_Expecter) GetTagsByDomain(domain interface{}) *mockDataSourceRepository_GetTagsByDomain_Call {
	return &mockDataSourceRepository_GetTagsByDomain_Call{Call: _e.mock.On("GetTagsByDomain", domain)}
}

func (_c *mockDataSourceRepository_GetTagsByDomain_Call) Run(run func(domain string)) *mockDataSourceRepository_GetTagsByDomain_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *mockDataSourceRepository_GetTagsByDomain_Call) Return(_a0 map[string][]*tag.Tag, _a1 error) *mockDataSourceRepository_GetTagsByDomain_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceRepository_GetTagsByDomain_Call) RunAndReturn(run func(string) (map[string][]*tag.Tag, error)) *mockDataSourceRepository_GetTagsByDomain_Call {
	_c.Call.Return(run)
	return _c
}

// GetTagsLinkedToDatabaseName provides a mock function with given fields: databaseName
func (_m *mockDataSourceRepository) GetTagsLinkedToDatabaseName(databaseName string) (map[string][]*tag.Tag, error) {
	ret := _m.Called(databaseName)

	if len(ret) == 0 {
		panic("no return value specified for GetTagsLinkedToDatabaseName")
	}

	var r0 map[string][]*tag.Tag
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (map[string][]*tag.Tag, error)); ok {
		return rf(databaseName)
	}
	if rf, ok := ret.Get(0).(func(string) map[string][]*tag.Tag); ok {
		r0 = rf(databaseName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string][]*tag.Tag)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(databaseName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataSourceRepository_GetTagsLinkedToDatabaseName_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetTagsLinkedToDatabaseName'
type mockDataSourceRepository_GetTagsLinkedToDatabaseName_Call struct {
	*mock.Call
}

// GetTagsLinkedToDatabaseName is a helper method to define mock.On call
//   - databaseName string
func (_e *mockDataSourceRepository_Expecter) GetTagsLinkedToDatabaseName(databaseName interface{}) *mockDataSourceRepository_GetTagsLinkedToDatabaseName_Call {
	return &mockDataSourceRepository_GetTagsLinkedToDatabaseName_Call{Call: _e.mock.On("GetTagsLinkedToDatabaseName", databaseName)}
}

func (_c *mockDataSourceRepository_GetTagsLinkedToDatabaseName_Call) Run(run func(databaseName string)) *mockDataSourceRepository_GetTagsLinkedToDatabaseName_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *mockDataSourceRepository_GetTagsLinkedToDatabaseName_Call) Return(_a0 map[string][]*tag.Tag, _a1 error) *mockDataSourceRepository_GetTagsLinkedToDatabaseName_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceRepository_GetTagsLinkedToDatabaseName_Call) RunAndReturn(run func(string) (map[string][]*tag.Tag, error)) *mockDataSourceRepository_GetTagsLinkedToDatabaseName_Call {
	_c.Call.Return(run)
	return _c
}

// GetWarehouses provides a mock function with no fields
func (_m *mockDataSourceRepository) GetWarehouses() ([]DbEntity, error) {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetWarehouses")
	}

	var r0 []DbEntity
	var r1 error
	if rf, ok := ret.Get(0).(func() ([]DbEntity, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() []DbEntity); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]DbEntity)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataSourceRepository_GetWarehouses_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetWarehouses'
type mockDataSourceRepository_GetWarehouses_Call struct {
	*mock.Call
}

// GetWarehouses is a helper method to define mock.On call
func (_e *mockDataSourceRepository_Expecter) GetWarehouses() *mockDataSourceRepository_GetWarehouses_Call {
	return &mockDataSourceRepository_GetWarehouses_Call{Call: _e.mock.On("GetWarehouses")}
}

func (_c *mockDataSourceRepository_GetWarehouses_Call) Run(run func()) *mockDataSourceRepository_GetWarehouses_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockDataSourceRepository_GetWarehouses_Call) Return(_a0 []DbEntity, _a1 error) *mockDataSourceRepository_GetWarehouses_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceRepository_GetWarehouses_Call) RunAndReturn(run func() ([]DbEntity, error)) *mockDataSourceRepository_GetWarehouses_Call {
	_c.Call.Return(run)
	return _c
}

// TotalQueryTime provides a mock function with no fields
func (_m *mockDataSourceRepository) TotalQueryTime() time.Duration {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for TotalQueryTime")
	}

	var r0 time.Duration
	if rf, ok := ret.Get(0).(func() time.Duration); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Duration)
	}

	return r0
}

// mockDataSourceRepository_TotalQueryTime_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'TotalQueryTime'
type mockDataSourceRepository_TotalQueryTime_Call struct {
	*mock.Call
}

// TotalQueryTime is a helper method to define mock.On call
func (_e *mockDataSourceRepository_Expecter) TotalQueryTime() *mockDataSourceRepository_TotalQueryTime_Call {
	return &mockDataSourceRepository_TotalQueryTime_Call{Call: _e.mock.On("TotalQueryTime")}
}

func (_c *mockDataSourceRepository_TotalQueryTime_Call) Run(run func()) *mockDataSourceRepository_TotalQueryTime_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockDataSourceRepository_TotalQueryTime_Call) Return(_a0 time.Duration) *mockDataSourceRepository_TotalQueryTime_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceRepository_TotalQueryTime_Call) RunAndReturn(run func() time.Duration) *mockDataSourceRepository_TotalQueryTime_Call {
	_c.Call.Return(run)
	return _c
}

// newMockDataSourceRepository creates a new instance of mockDataSourceRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockDataSourceRepository(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockDataSourceRepository {
	mock := &mockDataSourceRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
