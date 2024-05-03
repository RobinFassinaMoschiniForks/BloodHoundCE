package translate_test

import (
	"testing"

	"github.com/specterops/bloodhound/cypher/models/pgsql"
	"github.com/specterops/bloodhound/cypher/models/pgsql/format"
	"github.com/specterops/bloodhound/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

func TestInferExpressionType(t *testing.T) {
	type testCase struct {
		ExpectedType pgsql.DataType
		Expression   pgsql.Expression
	}

	testCases := []testCase{{
		ExpectedType: pgsql.Boolean,
		Expression: pgsql.NewBinaryExpression(
			pgsql.NewPropertyLookup(
				pgsql.CompoundIdentifier{"n", "properties"},
				pgsql.MustAsLiteral("field_a"),
			),
			pgsql.OperatorAnd,
			pgsql.NewBinaryExpression(
				pgsql.MustAsLiteral("123"),
				pgsql.OperatorIn,
				pgsql.ArrayLiteral{
					Values:   []pgsql.Expression{pgsql.MustAsLiteral("a"), pgsql.MustAsLiteral("b")},
					CastType: pgsql.TextArray,
				},
			),
		),
	}, {
		ExpectedType: pgsql.Boolean,
		Expression: pgsql.NewBinaryExpression(
			pgsql.NewPropertyLookup(
				pgsql.CompoundIdentifier{"n", "properties"},
				pgsql.MustAsLiteral("field_a"),
			),
			pgsql.OperatorAnd,
			pgsql.NewPropertyLookup(
				pgsql.CompoundIdentifier{"n", "properties"},
				pgsql.MustAsLiteral("field_b"),
			),
		),
	}, {
		ExpectedType: pgsql.Boolean,
		Expression: pgsql.NewBinaryExpression(
			pgsql.MustAsLiteral("123"),
			pgsql.OperatorIn,
			pgsql.ArrayLiteral{
				Values:   []pgsql.Expression{pgsql.MustAsLiteral("a"), pgsql.MustAsLiteral("b")},
				CastType: pgsql.TextArray,
			},
		),
	}, {
		ExpectedType: pgsql.Text,
		Expression: pgsql.NewBinaryExpression(
			pgsql.MustAsLiteral("123"),
			pgsql.OperatorConcatenate,
			pgsql.MustAsLiteral("456"),
		),
	}, {
		ExpectedType: pgsql.Int8,
		Expression: pgsql.NewBinaryExpression(
			pgsql.MustAsLiteral(123),
			pgsql.OperatorAdd,
			pgsql.NewBinaryExpression(
				pgsql.MustAsLiteral(123),
				pgsql.OperatorMultiply,
				pgsql.MustAsLiteral(1),
			),
		),
	}, {
		ExpectedType: pgsql.Int8,
		Expression: pgsql.NewBinaryExpression(
			pgsql.MustAsLiteral(123),
			pgsql.OperatorAdd,
			pgsql.NewBinaryExpression(
				pgsql.MustAsLiteral(int16(123)),
				pgsql.OperatorMultiply,
				pgsql.MustAsLiteral(int16(1)),
			),
		),
	}, {
		ExpectedType: pgsql.Int4,
		Expression: pgsql.NewBinaryExpression(
			pgsql.NewPropertyLookup(
				pgsql.CompoundIdentifier{"n", "properties"},
				pgsql.MustAsLiteral("field"),
			),
			pgsql.OperatorAdd,
			pgsql.NewBinaryExpression(
				pgsql.MustAsLiteral(int16(123)),
				pgsql.OperatorMultiply,
				pgsql.MustAsLiteral(int32(1)),
			),
		),
	}}

	for _, testCase := range testCases {
		if testName, err := format.Expression(testCase.Expression); err != nil {
			t.Fatalf("unable to format test case expression: %v", err)
		} else {
			t.Run(testName, func(t *testing.T) {
				inferredType, err := translate.InferExpressionType(testCase.Expression)

				require.Nil(t, err)
				require.Equal(t, testCase.ExpectedType, inferredType)
			})
		}
	}
}

func TestExpressionTreeTranslator(t *testing.T) {
	// Tree translator is a stack oriented expression tree builder
	treeTranslator := translate.NewExpressionTreeTranslator()

	// Case: Translating the constraint: a.name = 'a' and a.num_a > 1 and b.name = 'b' and a.other = b.other

	// Perform a prefix visit of the parent expression and its operator. This is used for tracking
	// conjunctions and disjunctions.
	treeTranslator.PushOperator(pgsql.OperatorEquals)

	// Postfix visit and push the compound identifier first: a.name
	treeTranslator.Push(pgsql.CompoundIdentifier{"a", "name"})

	// Postfix visit and push the literal next: "a"
	treeTranslator.Push(pgsql.MustAsLiteral("a"))

	// Perform a postfix visit of the parent expression and its operator.
	require.Nil(t, treeTranslator.PopPushOperator(pgsql.OperatorEquals))

	// Expect one newly created binary expression to be the only thing left on the tree
	// translator's operand stack
	require.Equal(t, 1, treeTranslator.Depth())
	require.IsType(t, &pgsql.BinaryExpression{}, treeTranslator.Peek())

	// Continue with: and a.num_a > 1
	// Preform a prefix visit of the 'and' operator:
	treeTranslator.PushOperator(pgsql.OperatorAnd)

	// Preform a prefix visit of the '>' operator:
	treeTranslator.PushOperator(pgsql.OperatorGreaterThan)

	// Postfix visit and push the compound identifier first: a.num_a
	treeTranslator.Push(pgsql.CompoundIdentifier{"a", "num_a"})

	// Postfix visit and push the literal next: 1
	treeTranslator.Push(pgsql.MustAsLiteral(1))

	// Perform a postfix visit of the parent expression and its operator.
	require.Nil(t, treeTranslator.PopPushOperator(pgsql.OperatorGreaterThan))

	// Perform a postfix visit of the conjoining parent expression and its operator.
	require.Nil(t, treeTranslator.PopPushOperator(pgsql.OperatorAnd))

	// Continue with: and b.name = "b"
	// Preform a prefix visit of the 'and' operator:
	treeTranslator.PushOperator(pgsql.OperatorAnd)

	// Preform a prefix visit of the '=' operator:
	treeTranslator.PushOperator(pgsql.OperatorEquals)

	// Postfix visit and push the compound identifier first: b.name
	treeTranslator.Push(pgsql.CompoundIdentifier{"b", "name"})

	// Postfix visit and push the literal next: "b"
	treeTranslator.Push(pgsql.MustAsLiteral("b"))

	// Perform a postfix visit of the parent expression and its operator.
	require.Nil(t, treeTranslator.PopPushOperator(pgsql.OperatorEquals))

	// Perform a postfix visit of the conjoining parent expression and its operator.
	require.Nil(t, treeTranslator.PopPushOperator(pgsql.OperatorAnd))

	// Continue with: and a.other = b.other
	// enter Op(and), enter Op(=)
	treeTranslator.PushOperator(pgsql.OperatorAnd)
	treeTranslator.PushOperator(pgsql.OperatorEquals)

	// push LOperand, push ROperand
	treeTranslator.Push(pgsql.CompoundIdentifier{"a", "other"})
	treeTranslator.Push(pgsql.CompoundIdentifier{"b", "other"})

	// exit  exit Op(=), Op(and)
	treeTranslator.PopPushOperator(pgsql.OperatorEquals)
	treeTranslator.PopPushOperator(pgsql.OperatorAnd)

	// Pull out the 'a' constraint
	aIdentifier := pgsql.AsIdentifierSet("a")
	expectedTranslation := "a.name = 'a' and a.num_a > 1"
	validateConstraints(t, treeTranslator, aIdentifier, expectedTranslation)

	// Pull out the 'b' constraint next
	bIdentifier := pgsql.AsIdentifierSet("b")
	expectedTranslation = "b.name = 'b'"
	validateConstraints(t, treeTranslator, bIdentifier, expectedTranslation)

	// Pull out the constraint that depends on both 'a' and 'b' identifiers
	idents := pgsql.AsIdentifierSet("a", "b")
	expectedTranslation = "a.other = b.other"
	validateConstraints(t, treeTranslator, idents, expectedTranslation)
}

func validateConstraints(t *testing.T, constraintTracker *translate.ExpressionTreeTranslator, idents *pgsql.IdentifierSet, expectedTranslation string) {
	constraint, err := constraintTracker.ConsumeSet(idents)

	require.NotNil(t, constraint)
	require.True(t, constraint.Dependencies.Matches(idents))
	require.Nil(t, err)

	formattedConstraint, err := format.Expression(constraint.Expression)

	require.Nil(t, err)
	require.Equal(t, expectedTranslation, formattedConstraint)
}

// match (a), (b) where a.name = 'a' OR b.name = 'b'
func TestExpressionTreeTranslator_Disjunction(t *testing.T) {
	exprTreeTranslator := translate.NewExpressionTreeTranslator()

	// Case: Translating the constraint: a.name = 'a' or b.name = 'b'

	// Start with: a.name = 'a'
	exprTreeTranslator.PushOperator(pgsql.OperatorEquals)
	exprTreeTranslator.Push(pgsql.CompoundIdentifier{"a", "name"})
	exprTreeTranslator.Push(pgsql.MustAsLiteral("a"))
	exprTreeTranslator.PopPushOperator(pgsql.OperatorEquals)

	require.Equal(t, 1, exprTreeTranslator.Depth())
	require.IsType(t, &pgsql.BinaryExpression{}, exprTreeTranslator.Peek())

	// Continue with: or b.name = 'b'
	exprTreeTranslator.PushOperator(pgsql.OperatorOr)
	exprTreeTranslator.PushOperator(pgsql.OperatorEquals)
	exprTreeTranslator.Push(pgsql.CompoundIdentifier{"b", "name"})
	exprTreeTranslator.Push(pgsql.MustAsLiteral("b"))
	exprTreeTranslator.PopPushOperator(pgsql.OperatorEquals)
	exprTreeTranslator.PopPushOperator(pgsql.OperatorOr)

	// we expect no constraints to have been extracted due to the presence of a disjunction
	require.Equal(t, 0, exprTreeTranslator.NumConstraints())

	// we expect the exprTree stack to contain only one element that represents the conjoined form of all visited operands
	conjoinedExpr := exprTreeTranslator.Peek()
	require.Equal(t, 1, exprTreeTranslator.Depth())
	require.IsType(t, &pgsql.BinaryExpression{}, conjoinedExpr)

	formattedExpr, err := format.Expression(conjoinedExpr)
	require.Nil(t, err)
	require.Equal(t, "a.name = 'a' or b.name = 'b'", formattedExpr)

	// eff it up. push a conjunction: and c.name = 'c'
	exprTreeTranslator.PushOperator(pgsql.OperatorAnd)
	exprTreeTranslator.PushOperator(pgsql.OperatorEquals)
	exprTreeTranslator.Push(pgsql.CompoundIdentifier{"c", "name"})
	exprTreeTranslator.Push(pgsql.MustAsLiteral("c"))
	exprTreeTranslator.PopPushOperator(pgsql.OperatorEquals)
	exprTreeTranslator.PopPushOperator(pgsql.OperatorAnd)

	// todo: there are 2 constraints and a disjunction depth of -1. constraints seem ok, but disjnction depth of -1?
	// a, b: a.name = 'a' or b.name = 'b'
	// c: c.name = 'c'

	// Pull out the constraint that depends on both 'a' and 'b' identifiers
	idents := pgsql.AsIdentifierSet("a", "b")
	expectedTranslation := "a.name = 'a' or b.name = 'b'"
	validateConstraints(t, exprTreeTranslator, idents, expectedTranslation)

	idents = pgsql.AsIdentifierSet("c")
	expectedTranslation = "c.name = 'c'"
	validateConstraints(t, exprTreeTranslator, idents, expectedTranslation)
}

// match (a), (b) where a.name = 'a' AND b.name = 'b'
func TestExpressionTreeTranslator_Conjunction(t *testing.T) {
	exprTreeTranslator := translate.NewExpressionTreeTranslator()

	// Case: Translating the constraint: a.name = 'a' AND b.name = 'b'

	// Start with: a.name = 'a'
	exprTreeTranslator.PushOperator(pgsql.OperatorEquals)
	exprTreeTranslator.Push(pgsql.CompoundIdentifier{"a", "name"})
	exprTreeTranslator.Push(pgsql.MustAsLiteral("a"))
	exprTreeTranslator.PopPushOperator(pgsql.OperatorEquals)

	require.Equal(t, 1, exprTreeTranslator.Depth())
	require.IsType(t, &pgsql.BinaryExpression{}, exprTreeTranslator.Peek())

	// Continue with: AND b.name = 'b'
	exprTreeTranslator.PushOperator(pgsql.OperatorAnd)
	exprTreeTranslator.PushOperator(pgsql.OperatorEquals)
	exprTreeTranslator.Push(pgsql.CompoundIdentifier{"b", "name"})
	exprTreeTranslator.Push(pgsql.MustAsLiteral("b"))
	exprTreeTranslator.PopPushOperator(pgsql.OperatorEquals)
	exprTreeTranslator.PopPushOperator(pgsql.OperatorAnd)

	// we expect 2 constraint to have been extracted
	require.Equal(t, 2, exprTreeTranslator.NumConstraints())

	// we expect the exprTree stack to be empty
	require.Equal(t, 0, exprTreeTranslator.Depth())

	// pull out 'a'
	ident := pgsql.AsIdentifierSet("a")
	validateConstraints(t, exprTreeTranslator, ident, "a.name = 'a'")

	// pull out 'b
	ident = pgsql.AsIdentifierSet("b")
	validateConstraints(t, exprTreeTranslator, ident, "b.name = 'b'")
}
