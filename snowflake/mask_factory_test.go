package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSimpleMaskGenerator_Generate(t *testing.T) {
	type args struct {
		maskName      string
		columnType    string
		beneficiaries MaskingBeneficiaries
	}
	type result struct {
		expectsError  bool
		maskingResult MaskingPolicy
	}

	tests := []struct {
		name         string
		args         args
		methodResult string
		result       result
	}{
		{
			name: "No beneficiaries",
			args: args{
				maskName:   "test_mask",
				columnType: "column_type",
				beneficiaries: MaskingBeneficiaries{
					Roles: []string{},
					Users: []string{},
				},
			},
			methodResult: "MASK(val)",
			result: result{
				expectsError:  false,
				maskingResult: "CREATE MASKING POLICY test_mask AS (val column_type) RETURNS column_type ->\nMASK(val);",
			},
		},
		{
			name: "single user beneficiary",
			args: args{
				maskName:   "test_mask",
				columnType: "column_type",
				beneficiaries: MaskingBeneficiaries{
					Roles: []string{},
					Users: []string{"test_user"},
				},
			},
			methodResult: "MASK(val)",
			result: result{
				expectsError:  false,
				maskingResult: "CREATE MASKING POLICY test_mask AS (val column_type) RETURNS column_type ->\nCASE\n\tWHEN current_user() IN ('test_user') THEN val\n\tELSE MASK(val)\nEND;",
			},
		},
		{
			name: "multiple users beneficiary",
			args: args{
				maskName:   "test_mask",
				columnType: "column_type",
				beneficiaries: MaskingBeneficiaries{
					Roles: []string{},
					Users: []string{"test_user1", "test_user2", "test_user3"},
				},
			},
			methodResult: "MASK(val)",
			result: result{
				expectsError:  false,
				maskingResult: "CREATE MASKING POLICY test_mask AS (val column_type) RETURNS column_type ->\nCASE\n\tWHEN current_user() IN ('test_user1', 'test_user2', 'test_user3') THEN val\n\tELSE MASK(val)\nEND;",
			},
		},
		{
			name: "single role beneficiary",
			args: args{
				maskName:   "test_mask",
				columnType: "column_type",
				beneficiaries: MaskingBeneficiaries{
					Roles: []string{"test_role"},
					Users: []string{},
				},
			},
			methodResult: "MASK(val)",
			result: result{
				expectsError:  false,
				maskingResult: "CREATE MASKING POLICY test_mask AS (val column_type) RETURNS column_type ->\nCASE\n\tWHEN (IS_ROLE_IN_SESSION('test_role')) THEN val\n\tELSE MASK(val)\nEND;",
			},
		},
		{
			name: "multiple roles beneficiary",
			args: args{
				maskName:   "test_mask",
				columnType: "column_type",
				beneficiaries: MaskingBeneficiaries{
					Roles: []string{"test_role1", "test_role2", "test_role3"},
					Users: []string{},
				},
			},
			methodResult: "MASK(val)",
			result: result{
				expectsError:  false,
				maskingResult: "CREATE MASKING POLICY test_mask AS (val column_type) RETURNS column_type ->\nCASE\n\tWHEN (IS_ROLE_IN_SESSION('test_role1') OR IS_ROLE_IN_SESSION('test_role2') OR IS_ROLE_IN_SESSION('test_role3')) THEN val\n\tELSE MASK(val)\nEND;",
			},
		},
		{
			name: "roles and users beneficiary",
			args: args{
				maskName:   "test_mask",
				columnType: "column_type",
				beneficiaries: MaskingBeneficiaries{
					Roles: []string{"test_role1", "test_role2", "test_role3"},
					Users: []string{"test_user1", "test_user2", "test_user3"},
				},
			},
			methodResult: "MASK2(val)",
			result: result{
				expectsError:  false,
				maskingResult: "CREATE MASKING POLICY test_mask AS (val column_type) RETURNS column_type ->\nCASE\n\tWHEN (IS_ROLE_IN_SESSION('test_role1') OR IS_ROLE_IN_SESSION('test_role2') OR IS_ROLE_IN_SESSION('test_role3')) THEN val\n\tWHEN current_user() IN ('test_user1', 'test_user2', 'test_user3') THEN val\n\tELSE MASK2(val)\nEND;",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			maskMethod := NewMockSimpleMaskMethod(t)
			maskMethod.EXPECT().MaskMethod("val").Return(tt.methodResult).Once()
			maskMethod.EXPECT().SupportedType(mock.AnythingOfType("string")).Return(true).Once()

			simpleMaskGen := SimpleMaskGenerator{SimpleMaskMethod: maskMethod}

			// When
			resultPolicy, err := simpleMaskGen.Generate(tt.args.maskName, tt.args.columnType, &tt.args.beneficiaries)

			// Then
			if tt.result.expectsError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.result.maskingResult, resultPolicy)
		})
	}

	t.Run("Unsupported column type", func(t *testing.T) {
		// Given
		maskMethod := NewMockSimpleMaskMethod(t)
		maskMethod.EXPECT().SupportedType(mock.AnythingOfType("string")).Return(false).Once()

		simpleMaskGen := SimpleMaskGenerator{SimpleMaskMethod: maskMethod}

		// When
		_, err := simpleMaskGen.Generate("test_mask", "column_type", nil)

		// Then
		require.Error(t, err)
	})
}

func TestNullMask_Generate(t *testing.T) {
	// Given
	nullMask := NullMask()
	beneficiaries := MaskingBeneficiaries{
		Roles: []string{"test_role1", "test_role2", "test_role3"},
		Users: []string{"test_user1", "test_user2", "test_user3"},
	}

	// When
	result, err := nullMask.Generate("test_mask", "column_type", &beneficiaries)

	// Then
	require.NoError(t, err)
	assert.Equal(t, MaskingPolicy("CREATE MASKING POLICY test_mask AS (val column_type) RETURNS column_type ->\nCASE\n\tWHEN (IS_ROLE_IN_SESSION('test_role1') OR IS_ROLE_IN_SESSION('test_role2') OR IS_ROLE_IN_SESSION('test_role3')) THEN val\n\tWHEN current_user() IN ('test_user1', 'test_user2', 'test_user3') THEN val\n\tELSE NULL\nEND;"), result)
}

func TestSha256Mask_Generate(t *testing.T) {
	hashMask := Sha256Mask()
	beneficiaries := MaskingBeneficiaries{
		Roles: []string{"test_role1", "test_role2", "test_role3"},
		Users: []string{"test_user1", "test_user2", "test_user3"},
	}

	// When
	result, err := hashMask.Generate("test_mask", "text", &beneficiaries)

	// Then
	require.NoError(t, err)
	assert.Equal(t, MaskingPolicy("CREATE MASKING POLICY test_mask AS (val text) RETURNS text ->\nCASE\n\tWHEN (IS_ROLE_IN_SESSION('test_role1') OR IS_ROLE_IN_SESSION('test_role2') OR IS_ROLE_IN_SESSION('test_role3')) THEN val\n\tWHEN current_user() IN ('test_user1', 'test_user2', 'test_user3') THEN val\n\tELSE SHA2(val, 256)\nEND;"), result)
}

func TestEncryptMask_Generate(t *testing.T) {
	// Given
	beneficiaries := MaskingBeneficiaries{
		Roles: []string{"test_role1", "test_role2"},
		Users: []string{"test_user1", "test_user2"},
	}

	tests := []struct {
		name      string
		generator MaskGenerator
		maskName  string
		result    string
	}{
		{
			name:      "encrypt with column tag",
			generator: EncryptMask("decryptIt", "encryption_type"),
			result:    "CREATE MASKING POLICY test_mask AS (val column_type) RETURNS column_type ->\nCASE\n\tWHEN (IS_ROLE_IN_SESSION('test_role1') OR IS_ROLE_IN_SESSION('test_role2')) THEN decryptIt(val, SYSTEM$GET_TAG_ON_CURRENT_COLUMN('encryption_type'))\n\tWHEN current_user() IN ('test_user1', 'test_user2') THEN decryptIt(val, SYSTEM$GET_TAG_ON_CURRENT_COLUMN('encryption_type'))\n\tELSE val\nEND;",
			maskName:  "test_mask",
		},
		{
			name:      "encrypt without column tag",
			generator: EncryptMask("decryptIt", ""),
			result:    "CREATE MASKING POLICY test_mask AS (val column_type) RETURNS column_type ->\nCASE\n\tWHEN (IS_ROLE_IN_SESSION('test_role1') OR IS_ROLE_IN_SESSION('test_role2')) THEN decryptIt(val)\n\tWHEN current_user() IN ('test_user1', 'test_user2') THEN decryptIt(val)\n\tELSE val\nEND;",
			maskName:  "test_mask",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When
			result, err := tt.generator.Generate(tt.maskName, "column_type", &beneficiaries)

			// Then
			require.NoError(t, err)
			assert.Equal(t, MaskingPolicy(tt.result), result)
		})
	}
}
