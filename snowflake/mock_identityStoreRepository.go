// Code generated by mockery v2.23.1. DO NOT EDIT.

package snowflake

import (
	time "time"

	mock "github.com/stretchr/testify/mock"
)

// mockIdentityStoreRepository is an autogenerated mock type for the identityStoreRepository type
type mockIdentityStoreRepository struct {
	mock.Mock
}

type mockIdentityStoreRepository_Expecter struct {
	mock *mock.Mock
}

func (_m *mockIdentityStoreRepository) EXPECT() *mockIdentityStoreRepository_Expecter {
	return &mockIdentityStoreRepository_Expecter{mock: &_m.Mock}
}

// Close provides a mock function with given fields:
func (_m *mockIdentityStoreRepository) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// mockIdentityStoreRepository_Close_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Close'
type mockIdentityStoreRepository_Close_Call struct {
	*mock.Call
}

// Close is a helper method to define mock.On call
func (_e *mockIdentityStoreRepository_Expecter) Close() *mockIdentityStoreRepository_Close_Call {
	return &mockIdentityStoreRepository_Close_Call{Call: _e.mock.On("Close")}
}

func (_c *mockIdentityStoreRepository_Close_Call) Run(run func()) *mockIdentityStoreRepository_Close_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockIdentityStoreRepository_Close_Call) Return(_a0 error) *mockIdentityStoreRepository_Close_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockIdentityStoreRepository_Close_Call) RunAndReturn(run func() error) *mockIdentityStoreRepository_Close_Call {
	_c.Call.Return(run)
	return _c
}

// GetUsers provides a mock function with given fields:
func (_m *mockIdentityStoreRepository) GetUsers() ([]UserEntity, error) {
	ret := _m.Called()

	var r0 []UserEntity
	var r1 error
	if rf, ok := ret.Get(0).(func() ([]UserEntity, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() []UserEntity); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]UserEntity)
		}
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockIdentityStoreRepository_GetUsers_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetUsers'
type mockIdentityStoreRepository_GetUsers_Call struct {
	*mock.Call
}

// GetUsers is a helper method to define mock.On call
func (_e *mockIdentityStoreRepository_Expecter) GetUsers() *mockIdentityStoreRepository_GetUsers_Call {
	return &mockIdentityStoreRepository_GetUsers_Call{Call: _e.mock.On("GetUsers")}
}

func (_c *mockIdentityStoreRepository_GetUsers_Call) Run(run func()) *mockIdentityStoreRepository_GetUsers_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockIdentityStoreRepository_GetUsers_Call) Return(_a0 []UserEntity, _a1 error) *mockIdentityStoreRepository_GetUsers_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *mockIdentityStoreRepository_GetUsers_Call) RunAndReturn(run func() ([]UserEntity, error)) *mockIdentityStoreRepository_GetUsers_Call {
	_c.Call.Return(run)
	return _c
}

// TotalQueryTime provides a mock function with given fields:
func (_m *mockIdentityStoreRepository) TotalQueryTime() time.Duration {
	ret := _m.Called()

	var r0 time.Duration
	if rf, ok := ret.Get(0).(func() time.Duration); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Duration)
	}

	return r0
}

// mockIdentityStoreRepository_TotalQueryTime_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'TotalQueryTime'
type mockIdentityStoreRepository_TotalQueryTime_Call struct {
	*mock.Call
}

// TotalQueryTime is a helper method to define mock.On call
func (_e *mockIdentityStoreRepository_Expecter) TotalQueryTime() *mockIdentityStoreRepository_TotalQueryTime_Call {
	return &mockIdentityStoreRepository_TotalQueryTime_Call{Call: _e.mock.On("TotalQueryTime")}
}

func (_c *mockIdentityStoreRepository_TotalQueryTime_Call) Run(run func()) *mockIdentityStoreRepository_TotalQueryTime_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *mockIdentityStoreRepository_TotalQueryTime_Call) Return(_a0 time.Duration) *mockIdentityStoreRepository_TotalQueryTime_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *mockIdentityStoreRepository_TotalQueryTime_Call) RunAndReturn(run func() time.Duration) *mockIdentityStoreRepository_TotalQueryTime_Call {
	_c.Call.Return(run)
	return _c
}

type mockConstructorTestingTnewMockIdentityStoreRepository interface {
	mock.TestingT
	Cleanup(func())
}

// newMockIdentityStoreRepository creates a new instance of mockIdentityStoreRepository. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func newMockIdentityStoreRepository(t mockConstructorTestingTnewMockIdentityStoreRepository) *mockIdentityStoreRepository {
	mock := &mockIdentityStoreRepository{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
