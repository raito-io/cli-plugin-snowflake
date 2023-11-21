package snowflake

import (
	"fmt"
	"strings"
)

var _maskFactory *MaskFactory

const (
	NullMaskId   = "NULL"
	SHA256MaskId = "SHA256"
)

func init() {
	_maskFactory = NewMaskFactory()
	_maskFactory.RegisterMaskGenerator(NullMaskId, NullMask())
	_maskFactory.RegisterMaskGenerator(SHA256MaskId, Sha256Mask())
}

//go:generate go run github.com/vektra/mockery/v2 --name=MaskGenerator --with-expecter --inpackage
type MaskGenerator interface {
	Generate(maskName string, columnType string, beneficiaries *MaskingBeneficiaries) (MaskingPolicy, error)
}

//go:generate go run github.com/vektra/mockery/v2 --name=SimpleMaskMethod --with-expecter --inpackage
type SimpleMaskMethod interface {
	MaskMethod(variableName string) string
	SupportedType(columnType string) bool
}

type SimpleMaskGenerator struct {
	SimpleMaskMethod
}

type MaskingBeneficiaries struct {
	Roles []string
	Users []string
}

type MaskFactory struct {
	maskGenerators map[string]MaskGenerator
}

type MaskingPolicy string

func NewMaskFactory() *MaskFactory {
	if _maskFactory == nil {
		_maskFactory = &MaskFactory{
			maskGenerators: make(map[string]MaskGenerator),
		}
	}

	return _maskFactory
}

func (f *MaskFactory) RegisterMaskGenerator(maskType string, maskGenerator MaskGenerator) {
	f.maskGenerators[maskType] = maskGenerator
}

func (f *MaskFactory) CreateMask(maskName string, columnType string, maskType *string, beneficiaries *MaskingBeneficiaries) (string, MaskingPolicy, error) {
	policyName := fmt.Sprintf("%s_%s", maskName, columnType)

	maskGen := NullMask()

	if maskType != nil {
		if gen, ok := f.maskGenerators[*maskType]; ok {
			maskGen = gen
		}
	}

	policy, err := maskGen.Generate(policyName, columnType, beneficiaries)
	if err != nil {
		maskGen = NullMask()

		policy, _ = maskGen.Generate(policyName, columnType, beneficiaries) // NULLMASK may never return an error and is used as a fallback
	}

	return policyName, policy, err
}

func NewSimpleMaskGenerator(method SimpleMaskMethod) *SimpleMaskGenerator {
	return &SimpleMaskGenerator{
		SimpleMaskMethod: method,
	}
}

func (g *SimpleMaskGenerator) Generate(maskName string, columnType string, beneficiaries *MaskingBeneficiaries) (MaskingPolicy, error) {
	if !g.SupportedType(columnType) {
		return "", fmt.Errorf("unsupported type %s", columnType)
	}

	var maskingPolicyBuilder strings.Builder

	maskingPolicyBuilder.WriteString(fmt.Sprintf("CREATE MASKING POLICY %[1]s AS (val %[2]s) RETURNS %[2]s ->\n", maskName, columnType))

	var cases []string

	if len(beneficiaries.Roles) > 0 {
		var roles []string
		for _, role := range beneficiaries.Roles {
			roles = append(roles, fmt.Sprintf("'%s'", role))
		}

		cases = append(cases, fmt.Sprintf("WHEN current_role() IN (%s) THEN val", strings.Join(roles, ", ")))
	}

	if len(beneficiaries.Users) > 0 {
		var users []string
		for _, user := range beneficiaries.Users {
			users = append(users, fmt.Sprintf("'%s'", user))
		}

		cases = append(cases, fmt.Sprintf("WHEN current_user() IN (%s) THEN val", strings.Join(users, ", ")))
	}

	maskFn := g.MaskMethod("val")

	if len(cases) == 0 {
		maskingPolicyBuilder.WriteString(maskFn)
	} else {
		maskingPolicyBuilder.WriteString("CASE\n")

		for _, c := range cases {
			maskingPolicyBuilder.WriteString(fmt.Sprintf("\t%s\n", c))
		}

		maskingPolicyBuilder.WriteString(fmt.Sprintf("\tELSE %s\n", maskFn))
		maskingPolicyBuilder.WriteString("END")
	}

	maskingPolicyBuilder.WriteString(";")

	return MaskingPolicy(maskingPolicyBuilder.String()), nil
}

//////////////
// NULL MASK//
//////////////

func NullMask() MaskGenerator {
	return NewSimpleMaskGenerator(&nullMaskMethod{})
}

type nullMaskMethod struct{}

func (m *nullMaskMethod) MaskMethod(_ string) string {
	return "NULL"
}

func (m *nullMaskMethod) SupportedType(_ string) bool {
	return true
}

//////////////////
// SHA-256 MASK //
//////////////////

func Sha256Mask() MaskGenerator {
	return NewSimpleMaskGenerator(&shaHashMaskMethod{digestSize: 256})
}

type shaHashMaskMethod struct {
	digestSize int
}

func (m *shaHashMaskMethod) MaskMethod(variableName string) string {
	return fmt.Sprintf("SHA2(%s, %d)", variableName, m.digestSize)
}

func (m *shaHashMaskMethod) SupportedType(columnType string) bool {
	columnType = strings.ToLower(columnType)
	if strings.HasPrefix(columnType, "varchar") || strings.HasPrefix(columnType, "char") || strings.HasPrefix(columnType, "string") || strings.HasPrefix(columnType, "text") {
		return true
	}

	return false
}
