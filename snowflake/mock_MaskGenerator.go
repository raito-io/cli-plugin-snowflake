// Code generated by mockery v2.45.0. DO NOT EDIT.

package snowflake

import mock "github.com/stretchr/testify/mock"

// MockMaskGenerator is an autogenerated mock type for the MaskGenerator type
type MockMaskGenerator struct {
	mock.Mock
}

type MockMaskGenerator_Expecter struct {
	mock *mock.Mock
}

func (_m *MockMaskGenerator) EXPECT() *MockMaskGenerator_Expecter {
	return &MockMaskGenerator_Expecter{mock: &_m.Mock}
}

// Generate provides a mock function with given fields: maskName, columnType, beneficiaries
func (_m *MockMaskGenerator) Generate(maskName string, columnType string, beneficiaries *MaskingBeneficiaries) (MaskingPolicy, error) {
	ret := _m.Called(maskName, columnType, beneficiaries)

	if len(ret) == 0 {
		panic("no return value specified for Generate")
	}

	var r0 MaskingPolicy
	var r1 error
	if rf, ok := ret.Get(0).(func(string, string, *MaskingBeneficiaries) (MaskingPolicy, error)); ok {
		return rf(maskName, columnType, beneficiaries)
	}
	if rf, ok := ret.Get(0).(func(string, string, *MaskingBeneficiaries) MaskingPolicy); ok {
		r0 = rf(maskName, columnType, beneficiaries)
	} else {
		r0 = ret.Get(0).(MaskingPolicy)
	}

	if rf, ok := ret.Get(1).(func(string, string, *MaskingBeneficiaries) error); ok {
		r1 = rf(maskName, columnType, beneficiaries)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockMaskGenerator_Generate_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Generate'
type MockMaskGenerator_Generate_Call struct {
	*mock.Call
}

// Generate is a helper method to define mock.On call
//   - maskName string
//   - columnType string
//   - beneficiaries *MaskingBeneficiaries
func (_e *MockMaskGenerator_Expecter) Generate(maskName interface{}, columnType interface{}, beneficiaries interface{}) *MockMaskGenerator_Generate_Call {
	return &MockMaskGenerator_Generate_Call{Call: _e.mock.On("Generate", maskName, columnType, beneficiaries)}
}

func (_c *MockMaskGenerator_Generate_Call) Run(run func(maskName string, columnType string, beneficiaries *MaskingBeneficiaries)) *MockMaskGenerator_Generate_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string), args[1].(string), args[2].(*MaskingBeneficiaries))
	})
	return _c
}

func (_c *MockMaskGenerator_Generate_Call) Return(_a0 MaskingPolicy, _a1 error) *MockMaskGenerator_Generate_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockMaskGenerator_Generate_Call) RunAndReturn(run func(string, string, *MaskingBeneficiaries) (MaskingPolicy, error)) *MockMaskGenerator_Generate_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockMaskGenerator creates a new instance of MockMaskGenerator. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockMaskGenerator(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockMaskGenerator {
	mock := &MockMaskGenerator{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
