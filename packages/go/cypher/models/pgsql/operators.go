package pgsql

type Operator string

func (s Operator) IsIn(others ...Operator) bool {
	for _, other := range others {
		if s == other {
			return true
		}
	}

	return false
}

func (s Operator) AsExpression() Expression {
	return s
}

func (s Operator) String() string {
	return string(s)
}

func (s Operator) NodeType() string {
	return "operator"
}

func OperatorIsIn(operator Expression, matchers ...Expression) bool {
	for _, matcher := range matchers {
		if operator == matcher {
			return true
		}
	}

	return false
}

func OperatorIsBoolean(operator Expression) bool {
	return OperatorIsIn(operator,
		OperatorAnd,
		OperatorOr,
		OperatorNot,
		OperatorEquals,
		OperatorNotEquals,
		OperatorGreaterThan,
		OperatorGreaterThanOrEqualTo,
		OperatorLessThan,
		OperatorLessThanOrEqualTo)
}

func OperatorIsPropertyLookup(operator Expression) bool {
	return OperatorIsIn(operator,
		OperatorPropertyLookup,
		OperatorJSONField,
		OperatorJSONTextField,
	)
}

const (
	UnsetOperator                Operator = ""
	OperatorUnion                Operator = "union"
	OperatorConcatenate          Operator = "||"
	OperatorArrayOverlap         Operator = "&&"
	OperatorEquals               Operator = "="
	OperatorNotEquals            Operator = "!="
	OperatorGreaterThan          Operator = ">"
	OperatorGreaterThanOrEqualTo Operator = ">="
	OperatorLessThan             Operator = "<"
	OperatorLessThanOrEqualTo    Operator = "<="
	OperatorLike                 Operator = "~~"
	OperatorILike                Operator = "ilike"
	OperatorPGArrayOverlap       Operator = "operator (pg_catalog.&&)"
	OperatorAnd                  Operator = "and"
	OperatorOr                   Operator = "or"
	OperatorNot                  Operator = "not"
	OperatorJSONBFieldExists     Operator = "?"
	OperatorJSONField            Operator = "->"
	OperatorJSONTextField        Operator = "->>"
	OperatorAdd                  Operator = "+"
	OperatorSubtract             Operator = "-"
	OperatorMultiply             Operator = "*"
	OperatorDivide               Operator = "/"
	OperatorIn                   Operator = "in"
	OperatorIs                   Operator = "is"
	OperatorIsNot                Operator = "is not"
	OperatorStartsWith           Operator = "starts with"
	OperatorContains             Operator = "contains"
	OperatorEndsWith             Operator = "ends with"
	OperatorPropertyLookup       Operator = "property_lookup"
	OperatorAdditionAssignment   Operator = "+="
	OperatorLabelAssignment      Operator = "label_assignment"

	OperatorAssignment = OperatorEquals
)
