// Code generated by mockery v2.38.0. DO NOT EDIT.

package snowflake

import (
	time "time"

	mock "github.com/stretchr/testify/mock"
)

// mockDataUsageRepository is an autogenerated mock type for the dataUsageRepository type
type mockDataUsageRepository struct {
	mock.Mock
}

type mockDataUsageRepository_Expecter struct {
	mock *mock.Mock
}

func (_m *mockDataUsageRepository) EXPECT() *mockDataUsageRepository_Expecter {
	return &mockDataUsageRepository_Expecter{mock: &_m.Mock}
}

// BatchingInformation provides a mock function with given fields: startDate, historyTable
func (_m *mockDataUsageRepository) BatchingInformation(startDate *time.Time, historyTable string) (*string, *string, int, error) {
	ret := _m.Called(startDate, historyTable)

	if len(ret) == 0 {
		panic("no return value specified for BatchingInformation")
	}

	var r0 *string
	var r1 *string
	var r2 int
	var r3 error
	if rf, ok := ret.Get(0).(func(*time.Time, string) (*string, *string, int, error)); ok {
		return rf(startDate, historyTable)
	}
	if rf, ok := ret.Get(0).(func(*time.Time, string) *string); ok {
		r0 = rf(startDate, historyTable)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*string)
		}
	}

	if rf, ok := ret.Get(1).(func(*time.Time, string) *string); ok {
		r1 = rf(startDate, historyTable)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(*string)
		}
	}

	if rf, ok := ret.Get(2).(func(*time.Time, string) int); ok {
		r2 = rf(startDate, historyTable)
	} else {
		r2 = ret.Get(2).(int)
	}

	if rf, ok := ret.Get(3).(func(*time.Time, string) error); ok {
		r3 = rf(startDate, historyTable)
	} else {
		r3 = ret.Error(3)
	}

	return r0, r1, r2, r3
}

// mockDataUsageRepository_BatchingInformation_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'BatchingInformation'
type mockDataUsageRepository_BatchingInformation_Call struct {
	*mock.Call
}

// BatchingInformation is a helper method to define mock.On call
//   - startDate *time.Time
//   - historyTable string
func (_e *mockDataUsageRepository_Expecter) BatchingInformation(startDate interface{}, historyTable interface{}) *mockDataUsageRepository_BatchingInformation_Call {
	return &mockDataUsageRepository_BatchingInformation_Call{Call: _e.mock.On("BatchingInformation", startDate, historyTable)}
}

func (_c *mockDataUsageRepository_BatchingInformation_Call) Run(run func(startDate *time.Time, historyTable string)) *mockDataUsageRepository_BatchingInformation_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*time.Time), args[1].(string))
	})
	return _c
}

func (_c *mockDataUsageRepository_BatchingInformation_Call) Return(_a0 *string, _a1 *string, _a2 int, _a3 error) *mockDataUsageRepository_BatchingInformation_Call {
	_c.Call.Return(_a0, _a1, _a2, _a3)
	return _c
}

func (_c *mockDataUsageRepository_BatchingInformation_Call) RunAndReturn(run func(*time.Time, string) (*string, *string, int, error)) *mockDataUsageRepository_BatchingInformation_Call {
	_c.Call.Return(run)
	return _c
}

// CheckAccessHistoryAvailability provides a mock function with given fields: historyTable
func (_m *mockDataUsageRepository) CheckAccessHistoryAvailability(historyTable string) (bool, error) {
	ret := _m.Called(historyTable)

	if len(ret) == 0 {
		panic("no return value specified for CheckAccessHistoryAvailability")
	}

	var r0 bool
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (bool, error)); ok {
		return rf(historyTable)
	}
	if rf, ok := ret.Get(0).(func(string) bool); ok {
		r0 = rf(historyTable)
	} else {
		r0 = ret.Get(0).(bool)
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(historyTable)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataUsageRepository_CheckAccessHistoryAvailability_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'CheckAccessHistoryAvailability'
type mockDataUsageRepository_CheckAccessHistoryAvailability_Call struct {
	*mock.Call
}

// CheckAccessHistoryAvailability is a helper method to define mock.On call
//   - historyTable string
func (_e *mockDataUsageRepository_Expecter) CheckAccessHistoryAvailability(historyTable interface{}) *mockDataUsageRepository_CheckAccessHistoryAvailability_Call {
	return &mockDataUsageRepository_CheckAccessHistoryAvailability_Call{Call: _e.mock.On("CheckAccessHistoryAvailability", historyTable)}
}

func (_c *mockDataUsageRepository_CheckAccessHistoryAvailability_Call) Run(run func(historyTable string)) *mockDataUsageRepository_CheckAccessHistoryAvailability_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *mockDataUsageRepository_CheckAccessHistoryAvailability_Call) Return(_a0 bool, _a1 error) *mockDataUsageRepository_CheckAccessHistoryAvailability_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataUsageRepository_CheckAccessHistoryAvailability_Call) RunAndReturn(run func(string) (bool, error)) *mockDataUsageRepository_CheckAccessHistoryAvailability_Call {
	_c.Call.Return(run)
	return _c
}

// Close provides a mock function with given fields:
func (_m *mockDataUsageRepository) Close() error {
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

// mockDataUsageRepository_Close_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Close'
type mockDataUsageRepository_Close_Call struct {
	*mock.Call
}

// Close is a helper method to define mock.On call
func (_e *mockDataUsageRepository_Expecter) Close() *mockDataUsageRepository_Close_Call {
	return &mockDataUsageRepository_Close_Call{Call: _e.mock.On("Close")}
}

func (_c *mockDataUsageRepository_Close_Call) Run(run func()) *mockDataUsageRepository_Close_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockDataUsageRepository_Close_Call) Return(_a0 error) *mockDataUsageRepository_Close_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataUsageRepository_Close_Call) RunAndReturn(run func() error) *mockDataUsageRepository_Close_Call {
	_c.Call.Return(run)
	return _c
}

// DataUsage provides a mock function with given fields: columns, limit, offset, historyTable, minTime, maxTime, accessHistoryAvailable
func (_m *mockDataUsageRepository) DataUsage(columns []string, limit int, offset int, historyTable string, minTime *string, maxTime *string, accessHistoryAvailable bool) ([]QueryDbEntities, error) {
	ret := _m.Called(columns, limit, offset, historyTable, minTime, maxTime, accessHistoryAvailable)

	if len(ret) == 0 {
		panic("no return value specified for DataUsage")
	}

	var r0 []QueryDbEntities
	var r1 error
	if rf, ok := ret.Get(0).(func([]string, int, int, string, *string, *string, bool) ([]QueryDbEntities, error)); ok {
		return rf(columns, limit, offset, historyTable, minTime, maxTime, accessHistoryAvailable)
	}
	if rf, ok := ret.Get(0).(func([]string, int, int, string, *string, *string, bool) []QueryDbEntities); ok {
		r0 = rf(columns, limit, offset, historyTable, minTime, maxTime, accessHistoryAvailable)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]QueryDbEntities)
		}
	}

	if rf, ok := ret.Get(1).(func([]string, int, int, string, *string, *string, bool) error); ok {
		r1 = rf(columns, limit, offset, historyTable, minTime, maxTime, accessHistoryAvailable)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDataUsageRepository_DataUsage_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DataUsage'
type mockDataUsageRepository_DataUsage_Call struct {
	*mock.Call
}

// DataUsage is a helper method to define mock.On call
//   - columns []string
//   - limit int
//   - offset int
//   - historyTable string
//   - minTime *string
//   - maxTime *string
//   - accessHistoryAvailable bool
func (_e *mockDataUsageRepository_Expecter) DataUsage(columns interface{}, limit interface{}, offset interface{}, historyTable interface{}, minTime interface{}, maxTime interface{}, accessHistoryAvailable interface{}) *mockDataUsageRepository_DataUsage_Call {
	return &mockDataUsageRepository_DataUsage_Call{Call: _e.mock.On("DataUsage", columns, limit, offset, historyTable, minTime, maxTime, accessHistoryAvailable)}
}

func (_c *mockDataUsageRepository_DataUsage_Call) Run(run func(columns []string, limit int, offset int, historyTable string, minTime *string, maxTime *string, accessHistoryAvailable bool)) *mockDataUsageRepository_DataUsage_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]string), args[1].(int), args[2].(int), args[3].(string), args[4].(*string), args[5].(*string), args[6].(bool))
	})
	return _c
}

func (_c *mockDataUsageRepository_DataUsage_Call) Return(_a0 []QueryDbEntities, _a1 error) *mockDataUsageRepository_DataUsage_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockDataUsageRepository_DataUsage_Call) RunAndReturn(run func([]string, int, int, string, *string, *string, bool) ([]QueryDbEntities, error)) *mockDataUsageRepository_DataUsage_Call {
	_c.Call.Return(run)
	return _c
}

// TotalQueryTime provides a mock function with given fields:
func (_m *mockDataUsageRepository) TotalQueryTime() time.Duration {
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

// mockDataUsageRepository_TotalQueryTime_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'TotalQueryTime'
type mockDataUsageRepository_TotalQueryTime_Call struct {
	*mock.Call
}

// TotalQueryTime is a helper method to define mock.On call
func (_e *mockDataUsageRepository_Expecter) TotalQueryTime() *mockDataUsageRepository_TotalQueryTime_Call {
	return &mockDataUsageRepository_TotalQueryTime_Call{Call: _e.mock.On("TotalQueryTime")}
}

func (_c *mockDataUsageRepository_TotalQueryTime_Call) Run(run func()) *mockDataUsageRepository_TotalQueryTime_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockDataUsageRepository_TotalQueryTime_Call) Return(_a0 time.Duration) *mockDataUsageRepository_TotalQueryTime_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockDataUsageRepository_TotalQueryTime_Call) RunAndReturn(run func() time.Duration) *mockDataUsageRepository_TotalQueryTime_Call {
	_c.Call.Return(run)
	return _c
}

// newMockDataUsageRepository creates a new instance of mockDataUsageRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func newMockDataUsageRepository(t interface {
	mock.TestingT
	Cleanup(func())
}) *mockDataUsageRepository {
	mock := &mockDataUsageRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
