// Code generated by mockery v2.23.1. DO NOT EDIT.

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

// Close provides a mock function with given fields:
func (_m *mockDataSourceRepository) Close() error {
	ret := _m.Called()

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

// GetColumnsInDatabase provides a mock function with given fields: databaseName, handleEntity
func (_m *mockDataSourceRepository) GetColumnsInDatabase(databaseName string, handleEntity EntityHandler) error {
	ret := _m.Called(databaseName, handleEntity)

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

// GetDataBases provides a mock function with given fields:
func (_m *mockDataSourceRepository) GetDataBases() ([]DbEntity, error) {
	ret := _m.Called()

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

// mockDataSourceRepository_GetDataBases_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetDataBases'
type mockDataSourceRepository_GetDataBases_Call struct {
	*mock.Call
}

// GetDataBases is a helper method to define mock.On call
func (_e *mockDataSourceRepository_Expecter) GetDataBases() *mockDataSourceRepository_GetDataBases_Call {
	return &mockDataSourceRepository_GetDataBases_Call{Call: _e.mock.On("GetDataBases")}
}

func (_c *mockDataSourceRepository_GetDataBases_Call) Run(run func()) *mockDataSourceRepository_GetDataBases_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockDataSourceRepository_GetDataBases_Call) Return(_a0 []DbEntity, _a1 error) *mockDataSourceRepository_GetDataBases_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceRepository_GetDataBases_Call) RunAndReturn(run func() ([]DbEntity, error)) *mockDataSourceRepository_GetDataBases_Call {
	_c.Call.Return(run)
	return _c
}

// GetSchemasInDatabase provides a mock function with given fields: databaseName, handleEntity
func (_m *mockDataSourceRepository) GetSchemasInDatabase(databaseName string, handleEntity EntityHandler) error {
	ret := _m.Called(databaseName, handleEntity)

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

// GetShares provides a mock function with given fields:
func (_m *mockDataSourceRepository) GetShares() ([]DbEntity, error) {
	ret := _m.Called()

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

// mockDataSourceRepository_GetShares_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetShares'
type mockDataSourceRepository_GetShares_Call struct {
	*mock.Call
}

// GetShares is a helper method to define mock.On call
func (_e *mockDataSourceRepository_Expecter) GetShares() *mockDataSourceRepository_GetShares_Call {
	return &mockDataSourceRepository_GetShares_Call{Call: _e.mock.On("GetShares")}
}

func (_c *mockDataSourceRepository_GetShares_Call) Run(run func()) *mockDataSourceRepository_GetShares_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockDataSourceRepository_GetShares_Call) Return(_a0 []DbEntity, _a1 error) *mockDataSourceRepository_GetShares_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceRepository_GetShares_Call) RunAndReturn(run func() ([]DbEntity, error)) *mockDataSourceRepository_GetShares_Call {
	_c.Call.Return(run)
	return _c
}

// GetSnowFlakeAccountName provides a mock function with given fields:
func (_m *mockDataSourceRepository) GetSnowFlakeAccountName() (string, error) {
	ret := _m.Called()

	var r0 string
	var r1 error
	if rf, ok := ret.Get(0).(func() (string, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
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
func (_e *mockDataSourceRepository_Expecter) GetSnowFlakeAccountName() *mockDataSourceRepository_GetSnowFlakeAccountName_Call {
	return &mockDataSourceRepository_GetSnowFlakeAccountName_Call{Call: _e.mock.On("GetSnowFlakeAccountName")}
}

func (_c *mockDataSourceRepository_GetSnowFlakeAccountName_Call) Run(run func()) *mockDataSourceRepository_GetSnowFlakeAccountName_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockDataSourceRepository_GetSnowFlakeAccountName_Call) Return(_a0 string, _a1 error) *mockDataSourceRepository_GetSnowFlakeAccountName_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceRepository_GetSnowFlakeAccountName_Call) RunAndReturn(run func() (string, error)) *mockDataSourceRepository_GetSnowFlakeAccountName_Call {
	_c.Call.Return(run)
	return _c
}

// GetTablesInDatabase provides a mock function with given fields: databaseName, schemaName, handleEntity
func (_m *mockDataSourceRepository) GetTablesInDatabase(databaseName string, schemaName string, handleEntity EntityHandler) error {
	ret := _m.Called(databaseName, schemaName, handleEntity)

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

// GetTags provides a mock function with given fields: databaseName
func (_m *mockDataSourceRepository) GetTags(databaseName string) (map[string][]*tag.Tag, error) {
	ret := _m.Called(databaseName)

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

// mockDataSourceRepository_GetTags_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetTags'
type mockDataSourceRepository_GetTags_Call struct {
	*mock.Call
}

// GetTags is a helper method to define mock.On call
//   - databaseName string
func (_e *mockDataSourceRepository_Expecter) GetTags(databaseName interface{}) *mockDataSourceRepository_GetTags_Call {
	return &mockDataSourceRepository_GetTags_Call{Call: _e.mock.On("GetTags", databaseName)}
}

func (_c *mockDataSourceRepository_GetTags_Call) Run(run func(databaseName string)) *mockDataSourceRepository_GetTags_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *mockDataSourceRepository_GetTags_Call) Return(_a0 map[string][]*tag.Tag, _a1 error) *mockDataSourceRepository_GetTags_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataSourceRepository_GetTags_Call) RunAndReturn(run func(string) (map[string][]*tag.Tag, error)) *mockDataSourceRepository_GetTags_Call {
	_c.Call.Return(run)
	return _c
}

// GetViewsInDatabase provides a mock function with given fields: databaseName, schemaName, handleEntity
func (_m *mockDataSourceRepository) GetViewsInDatabase(databaseName string, schemaName string, handleEntity EntityHandler) error {
	ret := _m.Called(databaseName, schemaName, handleEntity)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, string, EntityHandler) error); ok {
		r0 = rf(databaseName, schemaName, handleEntity)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// mockDataSourceRepository_GetViewsInDatabase_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetViewsInDatabase'
type mockDataSourceRepository_GetViewsInDatabase_Call struct {
	*mock.Call
}

// GetViewsInDatabase is a helper method to define mock.On call
//   - databaseName string
//   - schemaName string
//   - handleEntity EntityHandler
func (_e *mockDataSourceRepository_Expecter) GetViewsInDatabase(databaseName interface{}, schemaName interface{}, handleEntity interface{}) *mockDataSourceRepository_GetViewsInDatabase_Call {
	return &mockDataSourceRepository_GetViewsInDatabase_Call{Call: _e.mock.On("GetViewsInDatabase", databaseName, schemaName, handleEntity)}
}

func (_c *mockDataSourceRepository_GetViewsInDatabase_Call) Run(run func(databaseName string, schemaName string, handleEntity EntityHandler)) *mockDataSourceRepository_GetViewsInDatabase_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(EntityHandler))
	})
	return _c
}

func (_c *mockDataSourceRepository_GetViewsInDatabase_Call) Return(_a0 error) *mockDataSourceRepository_GetViewsInDatabase_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataSourceRepository_GetViewsInDatabase_Call) RunAndReturn(run func(string, string, EntityHandler) error) *mockDataSourceRepository_GetViewsInDatabase_Call {
	_c.Call.Return(run)
	return _c
}

// GetWarehouses provides a mock function with given fields:
func (_m *mockDataSourceRepository) GetWarehouses() ([]DbEntity, error) {
	ret := _m.Called()

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

// TotalQueryTime provides a mock function with given fields:
func (_m *mockDataSourceRepository) TotalQueryTime() time.Duration {
	ret := _m.Called()

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

type mockConstructorTestingTnewMockDataSourceRepository interface {
	mock.TestingT
	Cleanup(func())
}

// newMockDataSourceRepository creates a new instance of mockDataSourceRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func newMockDataSourceRepository(t mockConstructorTestingTnewMockDataSourceRepository) *mockDataSourceRepository {
	mock := &mockDataSourceRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
