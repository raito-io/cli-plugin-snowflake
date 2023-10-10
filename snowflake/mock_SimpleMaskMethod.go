// Code generated by mockery v2.34.0. DO NOT EDIT.

package snowflake

import mock "github.com/stretchr/testify/mock"

// MockSimpleMaskMethod is an autogenerated mock type for the SimpleMaskMethod type
type MockSimpleMaskMethod struct {
	mock.Mock
}

type MockSimpleMaskMethod_Expecter struct {
	mock *mock.Mock
}

func (_m *MockSimpleMaskMethod) EXPECT() *MockSimpleMaskMethod_Expecter {
	return &MockSimpleMaskMethod_Expecter{mock: &_m.Mock}
}

// MaskMethod provides a mock function with given fields: variableName
func (_m *MockSimpleMaskMethod) MaskMethod(variableName string) string {
	ret := _m.Called(variableName)

	var r0 string
	if rf, ok := ret.Get(0).(func(string) string); ok {
		r0 = rf(variableName)
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// MockSimpleMaskMethod_MaskMethod_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'MaskMethod'
type MockSimpleMaskMethod_MaskMethod_Call struct {
	*mock.Call
}

// MaskMethod is a helper method to define mock.On call
//   - variableName string
func (_e *MockSimpleMaskMethod_Expecter) MaskMethod(variableName interface{}) *MockSimpleMaskMethod_MaskMethod_Call {
	return &MockSimpleMaskMethod_MaskMethod_Call{Call: _e.mock.On("MaskMethod", variableName)}
}

func (_c *MockSimpleMaskMethod_MaskMethod_Call) Run(run func(variableName string)) *MockSimpleMaskMethod_MaskMethod_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *MockSimpleMaskMethod_MaskMethod_Call) Return(_a0 string) *MockSimpleMaskMethod_MaskMethod_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockSimpleMaskMethod_MaskMethod_Call) RunAndReturn(run func(string) string) *MockSimpleMaskMethod_MaskMethod_Call {
	_c.Call.Return(run)
	return _c
}

// SupportedType provides a mock function with given fields: columnType
func (_m *MockSimpleMaskMethod) SupportedType(columnType string) bool {
	ret := _m.Called(columnType)

	var r0 bool
	if rf, ok := ret.Get(0).(func(string) bool); ok {
		r0 = rf(columnType)
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

// MockSimpleMaskMethod_SupportedType_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SupportedType'
type MockSimpleMaskMethod_SupportedType_Call struct {
	*mock.Call
}

// SupportedType is a helper method to define mock.On call
//   - columnType string
func (_e *MockSimpleMaskMethod_Expecter) SupportedType(columnType interface{}) *MockSimpleMaskMethod_SupportedType_Call {
	return &MockSimpleMaskMethod_SupportedType_Call{Call: _e.mock.On("SupportedType", columnType)}
}

func (_c *MockSimpleMaskMethod_SupportedType_Call) Run(run func(columnType string)) *MockSimpleMaskMethod_SupportedType_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *MockSimpleMaskMethod_SupportedType_Call) Return(_a0 bool) *MockSimpleMaskMethod_SupportedType_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockSimpleMaskMethod_SupportedType_Call) RunAndReturn(run func(string) bool) *MockSimpleMaskMethod_SupportedType_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockSimpleMaskMethod creates a new instance of MockSimpleMaskMethod. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockSimpleMaskMethod(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockSimpleMaskMethod {
	mock := &MockSimpleMaskMethod{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
