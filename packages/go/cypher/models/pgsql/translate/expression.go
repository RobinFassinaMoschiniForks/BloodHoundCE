package translate

import (
	"fmt"
	"github.com/specterops/bloodhound/cypher/models/tree"

	"github.com/specterops/bloodhound/cypher/models/pgsql"
)

type PropertyLookup struct {
	Reference pgsql.CompoundIdentifier
	Field     pgsql.Literal
}

func asPropertyLookup(expression pgsql.Expression) (*pgsql.BinaryExpression, bool) {
	if binaryExpression, isBinaryExpression := expression.(*pgsql.BinaryExpression); isBinaryExpression {
		return binaryExpression, pgsql.OperatorIsPropertyLookup(binaryExpression.Operator)
	}

	return nil, false
}

func decomposePropertyLookup(expression pgsql.Expression) (PropertyLookup, error) {
	if propertyLookup, isPropertyLookup := asPropertyLookup(expression); !isPropertyLookup {
		return PropertyLookup{}, fmt.Errorf("expected binary expression for property lookup decomposition but found type: %T", expression)
	} else if reference, typeOK := propertyLookup.LOperand.(pgsql.CompoundIdentifier); !typeOK {
		return PropertyLookup{}, fmt.Errorf("expected left operand for property lookup to be a compound identifier but found type: %T", propertyLookup.LOperand)
	} else if field, typeOK := propertyLookup.ROperand.(pgsql.Literal); !typeOK {
		return PropertyLookup{}, fmt.Errorf("expected right operand for property lookup to be a literal but found type: %T", propertyLookup.ROperand)
	} else if field.CastType != pgsql.Text {
		return PropertyLookup{}, fmt.Errorf("expected property lookup field a string literal but found data type: %s", field.CastType)
	} else {
		return PropertyLookup{
			Reference: reference,
			Field:     field,
		}, nil
	}
}

func ExtractSyntaxNodeReferences(root pgsql.SyntaxNode) (*pgsql.IdentifierSet, error) {
	dependencies := pgsql.NewIdentifierSet()

	return dependencies, tree.WalkPgSQL(root, tree.NewSimpleVisitor[pgsql.SyntaxNode](
		func(node pgsql.SyntaxNode, errorHandler tree.CancelableErrorHandler) {
			switch typedNode := node.(type) {
			case pgsql.CompoundIdentifier:
				dependencies.Add(typedNode.Root())
			}
		},
	))
}

func applyUnaryExpressionTypeHints(expression *pgsql.UnaryExpression) error {
	if propertyLookup, isPropertyLookup := asPropertyLookup(expression.Operand); isPropertyLookup {
		expression.Operand = rewritePropertyLookupOperator(propertyLookup, pgsql.Boolean)
	}

	return nil
}

func rewritePropertyLookupOperator(propertyLookup *pgsql.BinaryExpression, dataType pgsql.DataType) pgsql.Expression {
	// If this binary expression does not have the IR property lookup operator
	switch dataType {
	case pgsql.Text:
		propertyLookup.Operator = pgsql.OperatorJSONTextField
		return propertyLookup

	case pgsql.Date, pgsql.TimestampWithoutTimeZone, pgsql.TimestampWithTimeZone, pgsql.TimeWithoutTimeZone, pgsql.TimeWithTimeZone:
		propertyLookup.Operator = pgsql.OperatorJSONTextField
		return pgsql.NewTypeCast(propertyLookup, dataType)

	case pgsql.UnknownDataType:
		propertyLookup.Operator = pgsql.OperatorJSONField
		return propertyLookup

	default:
		propertyLookup.Operator = pgsql.OperatorJSONField
		return pgsql.NewTypeCast(propertyLookup, dataType)
	}
}

func GetTypeHint(expression pgsql.Expression) (pgsql.DataType, bool) {
	if typeHintedExpression, isTypeHinted := expression.(pgsql.TypeHinted); isTypeHinted {
		return typeHintedExpression.TypeHint(), true
	}

	return pgsql.UnsetDataType, false
}

func inferBinaryExpressionType(expression *pgsql.BinaryExpression) (pgsql.DataType, error) {
	var (
		leftHint, isLeftHinted   = GetTypeHint(expression.LOperand)
		rightHint, isRightHinted = GetTypeHint(expression.ROperand)
	)

	if isLeftHinted {
		if isRightHinted {
			if higherLevelHint, matchesOrConverts := leftHint.Convert(rightHint); !matchesOrConverts {
				return pgsql.UnsetDataType, fmt.Errorf("left and right operands for binary expression \"%s\" are not compatible: %s != %s", expression.Operator, leftHint, rightHint)
			} else {
				return higherLevelHint, nil
			}
		} else if inferredRightHint, err := InferExpressionType(expression.ROperand); err != nil {
			return pgsql.UnsetDataType, err
		} else if inferredRightHint == pgsql.UnknownDataType {
			// Assume the right side is convertable and return the left operand hint
			return leftHint, nil
		} else if upcastHint, matchesOrConverts := leftHint.Convert(inferredRightHint); !matchesOrConverts {
			return pgsql.UnsetDataType, fmt.Errorf("left and right operands for binary expression \"%s\" are not compatible: %s != %s", expression.Operator, leftHint, inferredRightHint)
		} else {
			return upcastHint, nil
		}
	} else if isRightHinted {
		// There's no left type, attempt to infer it
		if inferredLeftHint, err := InferExpressionType(expression.ROperand); err != nil {
			return pgsql.UnsetDataType, err
		} else if inferredLeftHint == pgsql.UnknownDataType {
			// Assume the right side is convertable and return the left operand hint
			return rightHint, nil
		} else if upcastHint, matchesOrConverts := rightHint.Convert(inferredLeftHint); !matchesOrConverts {
			return pgsql.UnsetDataType, fmt.Errorf("left and right operands for binary expression \"%s\" are not compatible: %s != %s", expression.Operator, leftHint, inferredLeftHint)
		} else {
			return upcastHint, nil
		}
	} else if inferredLeftHint, err := InferExpressionType(expression.LOperand); err != nil {
		return pgsql.UnsetDataType, err
	} else if inferredRightHint, err := InferExpressionType(expression.ROperand); err != nil {
		return pgsql.UnsetDataType, err
	} else if inferredLeftHint == pgsql.UnknownDataType && inferredRightHint == pgsql.UnknownDataType {
		// If neither side has type information then check the operator to see if it implies some type hinting
		switch expression.Operator {
		case pgsql.OperatorStartsWith, pgsql.OperatorContains, pgsql.OperatorEndsWith:
			// String operations imply the operands must be text
			return pgsql.Text, nil

		// TODO: Boolean inference for OperatorAnd and OperatorOr may want to be plumbed here

		default:
			// Unable to infer any type information
			return pgsql.UnknownDataType, nil
		}
	} else if higherLevelHint, matchesOrConverts := inferredLeftHint.Convert(inferredRightHint); !matchesOrConverts {
		return pgsql.UnsetDataType, fmt.Errorf("left and right operands for binary expression \"%s\" are not compatible: %s != %s", expression.Operator, leftHint, inferredLeftHint)
	} else {
		return higherLevelHint, nil
	}
}

func InferExpressionType(expression pgsql.Expression) (pgsql.DataType, error) {
	switch typedExpression := expression.(type) {
	case pgsql.Identifier, pgsql.CompoundExpression:
		// TODO: Type inference may be aided by searching the bound scope for a data type
		return pgsql.UnknownDataType, nil

	case pgsql.CompoundIdentifier:
		if len(typedExpression) != 2 {
			return pgsql.UnsetDataType, fmt.Errorf("expected a compound identifier to have only 2 components but found: %d", len(typedExpression))
		}

		// Infer type information for well known column names
		switch typedExpression[1] {
		case pgsql.ColumnGraphID, pgsql.ColumnID, pgsql.ColumnStartID, pgsql.ColumnEndID:
			return pgsql.Int4, nil

		case pgsql.ColumnKindID:
			return pgsql.Int2, nil

		case pgsql.ColumnKindIDs:
			return pgsql.Int4Array, nil

		case pgsql.ColumnProperties:
			return pgsql.JSONB, nil

		default:
			return pgsql.UnknownDataType, nil
		}

	case pgsql.TypeHinted:
		return typedExpression.TypeHint(), nil

	case *pgsql.BinaryExpression:
		switch typedExpression.Operator {
		case pgsql.OperatorPropertyLookup, pgsql.OperatorJSONField, pgsql.OperatorJSONTextField:
			// This is unknown, not unset meaning that it can be re-cast by future inference inspections
			return pgsql.UnknownDataType, nil

		case pgsql.OperatorAnd, pgsql.OperatorOr:
			return pgsql.Boolean, nil

		default:
			return inferBinaryExpressionType(typedExpression)
		}

	default:
		return pgsql.UnsetDataType, fmt.Errorf("unable to infer type hint for expression type: %T", expression)
	}
}

func applyBinaryExpressionTypeHints(expression *pgsql.BinaryExpression) error {
	if pgsql.OperatorIsPropertyLookup(expression.Operator) {
		// Don't directly hint property lookups
		return nil
	}

	if expressionTypeHint, err := InferExpressionType(expression); err != nil {
		return err
	} else {
		if leftPropertyLookup, isPropertyLookup := asPropertyLookup(expression.LOperand); isPropertyLookup {
			expression.LOperand = rewritePropertyLookupOperator(leftPropertyLookup, expressionTypeHint)
		}

		if rightPropertyLookup, isPropertyLookup := asPropertyLookup(expression.ROperand); isPropertyLookup {
			expression.ROperand = rewritePropertyLookupOperator(rightPropertyLookup, expressionTypeHint)
		}

		return nil
	}
}

type Builder struct {
	stack []pgsql.Expression
}

func NewExpressionTreeBuilder() *Builder {
	return &Builder{}
}

func (s *Builder) Depth() int {
	return len(s.stack)
}

func (s *Builder) IsEmpty() bool {
	return len(s.stack) == 0
}

func (s *Builder) Pop() (pgsql.Expression, error) {
	next := s.stack[len(s.stack)-1]
	s.stack = s.stack[:len(s.stack)-1]

	switch typedNext := next.(type) {
	case *pgsql.UnaryExpression:
		if err := applyUnaryExpressionTypeHints(typedNext); err != nil {
			return nil, err
		}

	case *pgsql.BinaryExpression:
		if err := applyBinaryExpressionTypeHints(typedNext); err != nil {
			return nil, err
		}
	}

	return next, nil
}

func (s *Builder) Peek() pgsql.Expression {
	return s.stack[len(s.stack)-1]
}

func (s *Builder) Push(expression pgsql.Expression) {
	s.stack = append(s.stack, expression)
}

type ExpressionTreeBuilder interface {
	Pop() (pgsql.Expression, error)
	Peek() pgsql.Expression
	Push(expression pgsql.Expression)
}

func PopFromBuilderAs[T any](builder ExpressionTreeBuilder) (T, error) {
	var empty T

	if value, err := builder.Pop(); err != nil {
		return empty, err
	} else if typed, isType := value.(T); isType {
		return typed, nil
	} else {
		return empty, fmt.Errorf("unable to convert type %T as %T", value, empty)
	}
}

func ConjoinExpressions(expressions []pgsql.Expression) (pgsql.Expression, error) {
	var conjoined pgsql.Expression

	for _, expression := range expressions {
		if expression == nil {
			continue
		}

		if conjoined == nil {
			conjoined = expression
			continue
		}

		conjoinedBinaryExpression := pgsql.NewBinaryExpression(conjoined, pgsql.OperatorAnd, expression)

		if err := applyBinaryExpressionTypeHints(conjoinedBinaryExpression); err != nil {
			return nil, err
		}

		conjoined = conjoinedBinaryExpression
	}

	return conjoined, nil
}

type ExpressionTreeTranslator struct {
	projectionConstraints    []*Constraint
	identifierSetConstraints *ConstraintTracker
	treeBuilder              *Builder
	disjunctionDepth         int
	conjunctionDepth         int
}

func NewExpressionTreeTranslator() *ExpressionTreeTranslator {
	return &ExpressionTreeTranslator{
		identifierSetConstraints: NewConstraintTracker(),
		treeBuilder:              NewExpressionTreeBuilder(),
	}
}

func (s *ExpressionTreeTranslator) Consume(identifier pgsql.Identifier) (Constraint, error) {
	return s.identifierSetConstraints.ConsumeSet(pgsql.AsIdentifierSet(identifier))
}

func (s *ExpressionTreeTranslator) ConsumeSet(identifierSet *pgsql.IdentifierSet) (Constraint, error) {
	return s.identifierSetConstraints.ConsumeSet(identifierSet)
}

func (s *ExpressionTreeTranslator) ConsumeAll() (Constraint, error) {
	if constraint, err := s.identifierSetConstraints.ConsumeAll(); err != nil {
		return Constraint{}, err
	} else {
		constraintExpressions := []pgsql.Expression{constraint.Expression}

		for _, projectionConstraint := range s.projectionConstraints {
			constraint.Dependencies.MergeSet(projectionConstraint.Dependencies)
			constraintExpressions = append(constraintExpressions, projectionConstraint.Expression)
		}

		if conjoined, err := ConjoinExpressions(constraintExpressions); err != nil {
			return Constraint{}, err
		} else {
			constraint.Expression = conjoined
		}

		return constraint, nil
	}
}

func (s *ExpressionTreeTranslator) Constrain(identifierSet *pgsql.IdentifierSet, expression pgsql.Expression) error {
	switch typedExpression := expression.(type) {
	case *pgsql.UnaryExpression:
		if err := applyUnaryExpressionTypeHints(typedExpression); err != nil {
			return err
		}

	case *pgsql.BinaryExpression:
		if err := applyBinaryExpressionTypeHints(typedExpression); err != nil {
			return err
		}
	}

	return s.identifierSetConstraints.Constrain(identifierSet, expression)
}

func (s *ExpressionTreeTranslator) ConstrainIdentifier(identifier pgsql.Identifier, expression pgsql.Expression) error {
	if identifierDeps, err := ExtractSyntaxNodeReferences(expression); err != nil {
		return err
	} else {
		return s.Constrain(identifierDeps, expression)
	}
}

func (s *ExpressionTreeTranslator) Depth() int {
	return s.treeBuilder.Depth()
}

func (s *ExpressionTreeTranslator) Push(expression pgsql.Expression) {
	s.treeBuilder.Push(expression)
}

func (s *ExpressionTreeTranslator) Peek() pgsql.Expression {
	return s.treeBuilder.Peek()
}

func (s *ExpressionTreeTranslator) NumConstraints() int {
	return len(s.identifierSetConstraints.Constraints)
}

func (s *ExpressionTreeTranslator) Pop() (pgsql.Expression, error) {
	return s.treeBuilder.Pop()
}

func (s *ExpressionTreeTranslator) popOperandAsConstraint() error {
	if operand, err := s.Pop(); err != nil {
		return err
	} else if identifierDeps, err := ExtractSyntaxNodeReferences(operand); err != nil {
		return err
	} else {
		return s.Constrain(identifierDeps, operand)
	}
}

func (s *ExpressionTreeTranslator) ConstrainRemainingOperands() error {
	// Pull the right operand only if one exists
	for !s.treeBuilder.IsEmpty() {
		if err := s.popOperandAsConstraint(); err != nil {
			return err
		}
	}

	return nil
}

func (s *ExpressionTreeTranslator) ConstrainDisjointOperandPair() error {
	// Always expect a left operand
	if s.treeBuilder.IsEmpty() {
		return fmt.Errorf("expected at least one operand for constraint extraction")
	}

	if rightOperand, err := s.treeBuilder.Pop(); err != nil {
		return err
	} else if rightDependencies, err := ExtractSyntaxNodeReferences(rightOperand); err != nil {
		return err
	} else if leftOperand, err := s.treeBuilder.Pop(); err != nil {
		return err
	} else if leftDependencies, err := ExtractSyntaxNodeReferences(leftOperand); err != nil {
		return err
	} else {
		var (
			combinedDependencies = leftDependencies.Copy().MergeSet(rightDependencies)
			projectionConstraint = pgsql.NewBinaryExpression(
				leftOperand,
				pgsql.OperatorOr,
				rightOperand,
			)
		)

		if err := applyBinaryExpressionTypeHints(projectionConstraint); err != nil {
			return err
		}

		return s.Constrain(combinedDependencies, projectionConstraint)
	}
}

func (s *ExpressionTreeTranslator) ConstrainConjoinedOperandPair() error {
	// Always expect a left operand
	if s.treeBuilder.IsEmpty() {
		return fmt.Errorf("expected at least one operand for constraint extraction")
	}

	if err := s.popOperandAsConstraint(); err != nil {
		return err
	}

	if !s.treeBuilder.IsEmpty() {
		// Pull the right operand only if one exists
		return s.popOperandAsConstraint()
	}

	return nil
}

func (s *ExpressionTreeTranslator) PopBinaryExpression(operator pgsql.Operator) (*pgsql.BinaryExpression, error) {
	if rightOperand, err := s.Pop(); err != nil {
		return nil, err
	} else if leftOperand, err := s.Pop(); err != nil {
		return nil, err
	} else {
		newBinaryExpression := pgsql.NewBinaryExpression(leftOperand, operator, rightOperand)
		return newBinaryExpression, applyBinaryExpressionTypeHints(newBinaryExpression)
	}
}

func (s *ExpressionTreeTranslator) PopPushBinaryExpression(operator pgsql.Operator) error {
	if newExpression, err := s.PopBinaryExpression(operator); err != nil {
		return err
	} else {
		switch operator {
		case pgsql.OperatorContains:
			switch typedLOperand := newExpression.LOperand.(type) {
			case *pgsql.BinaryExpression:
				switch typedLOperand.Operator {
				case pgsql.OperatorPropertyLookup, pgsql.OperatorJSONField, pgsql.OperatorJSONTextField:
				default:
					return fmt.Errorf("unexpected operator %s for binary expression \"%s\" left operand", typedLOperand.Operator, operator)
				}

				switch typedROperand := newExpression.ROperand.(type) {
				case pgsql.Literal:
					if rOperandDataType := typedROperand.TypeHint(); rOperandDataType != pgsql.Text {
						return fmt.Errorf("expected %s data type but found %s as right operand for operator %s", pgsql.Text, rOperandDataType, operator)
					} else if stringValue, isString := typedROperand.Value.(string); !isString {
						return fmt.Errorf("expected string but found %T as right operand for operator %s", typedROperand.Value, operator)
					} else if newLiteralValue, err := pgsql.AsLiteral("%" + stringValue + "%"); err != nil {
						return err
					} else {
						newExpression.Operator = pgsql.OperatorLike
						newExpression.ROperand = newLiteralValue
					}

				case *pgsql.BinaryExpression:
					if stringLiteral, err := pgsql.AsLiteral("%"); err != nil {
						return err
					} else {
						if pgsql.OperatorIsPropertyLookup(typedROperand.Operator) {
							typedROperand.Operator = pgsql.OperatorJSONTextField
						}

						newExpression.Operator = pgsql.OperatorLike
						newExpression.ROperand = pgsql.NewTypeCast(pgsql.NewBinaryExpression(
							stringLiteral,
							pgsql.OperatorConcatenate,
							pgsql.NewBinaryExpression(
								pgsql.Parenthetical{
									Expression: typedROperand,
								},
								pgsql.OperatorConcatenate,
								stringLiteral,
							),
						), pgsql.Text)
					}

				default:
					return fmt.Errorf("unexpected right operand %T for operator %s", newExpression.ROperand, operator)
				}
			}

			s.Push(newExpression)

		case pgsql.OperatorStartsWith:
			switch typedLOperand := newExpression.LOperand.(type) {
			case *pgsql.BinaryExpression:
				switch typedLOperand.Operator {
				case pgsql.OperatorPropertyLookup, pgsql.OperatorJSONField, pgsql.OperatorJSONTextField:
				default:
					return fmt.Errorf("unexpected operator %s for binary expression \"%s\" left operand", typedLOperand.Operator, operator)
				}

				switch typedROperand := newExpression.ROperand.(type) {
				case pgsql.Literal:
					if rOperandDataType := typedROperand.TypeHint(); rOperandDataType != pgsql.Text {
						return fmt.Errorf("expected %s data type but found %s as right operand for operator %s", pgsql.Text, rOperandDataType, operator)
					} else if stringValue, isString := typedROperand.Value.(string); !isString {
						return fmt.Errorf("expected string but found %T as right operand for operator %s", typedROperand.Value, operator)
					} else if newLiteralValue, err := pgsql.AsLiteral(stringValue + "%"); err != nil {
						return err
					} else {
						newExpression.Operator = pgsql.OperatorLike
						newExpression.ROperand = newLiteralValue
					}

				case *pgsql.BinaryExpression:
					if stringLiteral, err := pgsql.AsLiteral("%"); err != nil {
						return err
					} else {
						if pgsql.OperatorIsPropertyLookup(typedROperand.Operator) {
							typedROperand.Operator = pgsql.OperatorJSONTextField
						}

						newExpression.Operator = pgsql.OperatorLike
						newExpression.ROperand = pgsql.NewTypeCast(pgsql.NewBinaryExpression(
							pgsql.Parenthetical{
								Expression: typedROperand,
							},
							pgsql.OperatorConcatenate,
							stringLiteral,
						), pgsql.Text)
					}

				default:
					return fmt.Errorf("unexpected right operand %T for operator %s", newExpression.ROperand, operator)
				}
			}

			s.Push(newExpression)

		case pgsql.OperatorEndsWith:
			switch typedLOperand := newExpression.LOperand.(type) {
			case *pgsql.BinaryExpression:
				switch typedLOperand.Operator {
				case pgsql.OperatorPropertyLookup, pgsql.OperatorJSONField, pgsql.OperatorJSONTextField:
				default:
					return fmt.Errorf("unexpected operator %s for binary expression \"%s\" left operand", typedLOperand.Operator, operator)
				}

				switch typedROperand := newExpression.ROperand.(type) {
				case pgsql.Literal:
					if rOperandDataType := typedROperand.TypeHint(); rOperandDataType != pgsql.Text {
						return fmt.Errorf("expected %s data type but found %s as right operand for operator %s", pgsql.Text, rOperandDataType, operator)
					} else if stringValue, isString := typedROperand.Value.(string); !isString {
						return fmt.Errorf("expected string but found %T as right operand for operator %s", typedROperand.Value, operator)
					} else if newLiteralValue, err := pgsql.AsLiteral("%" + stringValue); err != nil {
						return err
					} else {
						newExpression.Operator = pgsql.OperatorLike
						newExpression.ROperand = newLiteralValue
					}

				case *pgsql.BinaryExpression:
					if stringLiteral, err := pgsql.AsLiteral("%"); err != nil {
						return err
					} else {
						if pgsql.OperatorIsPropertyLookup(typedROperand.Operator) {
							typedROperand.Operator = pgsql.OperatorJSONTextField
						}

						newExpression.Operator = pgsql.OperatorLike
						newExpression.ROperand = pgsql.NewTypeCast(pgsql.NewBinaryExpression(
							stringLiteral,
							pgsql.OperatorConcatenate,
							pgsql.Parenthetical{
								Expression: typedROperand,
							},
						), pgsql.Text)
					}

				default:
					return fmt.Errorf("unexpected right operand %T for operator %s", newExpression.ROperand, operator)
				}
			}

			s.Push(newExpression)

		case pgsql.OperatorIs:
			switch typedLOperand := newExpression.LOperand.(type) {
			case *pgsql.BinaryExpression:
				switch typedLOperand.Operator {
				case pgsql.OperatorPropertyLookup, pgsql.OperatorJSONField, pgsql.OperatorJSONTextField:
					// This is a null-check against a property. This should be rewritten using the JSON field exists
					// operator instead. It can be
					switch typedROperand := newExpression.ROperand.(type) {
					case pgsql.Literal:
						if typedROperand.Null {
							newExpression.Operator = pgsql.OperatorJSONBFieldExists
							newExpression.LOperand = typedLOperand.LOperand
							newExpression.ROperand = typedLOperand.ROperand
						}

						s.Push(pgsql.NewUnaryExpression(pgsql.OperatorNot, newExpression))
					}
				}
			}

		case pgsql.OperatorIsNot:
			switch typedLOperand := newExpression.LOperand.(type) {
			case *pgsql.BinaryExpression:
				switch typedLOperand.Operator {
				case pgsql.OperatorPropertyLookup, pgsql.OperatorJSONField, pgsql.OperatorJSONTextField:
					// This is a null-check against a property. This should be rewritten using the JSON field exists
					// operator instead. It can be
					switch typedROperand := newExpression.ROperand.(type) {
					case pgsql.Literal:
						if typedROperand.Null {
							newExpression.Operator = pgsql.OperatorJSONBFieldExists
							newExpression.LOperand = typedLOperand.LOperand
							newExpression.ROperand = typedLOperand.ROperand
						}

						s.Push(newExpression)
					}
				}
			}

		case pgsql.OperatorIn:
			// TODO: This may not always hold true if the right side is a sub-query
			newExpression.Operator = pgsql.OperatorEquals

			switch typedROperand := newExpression.ROperand.(type) {
			case pgsql.TypeCast:
				switch typedInnerOperand := typedROperand.Expression.(type) {
				case *pgsql.BinaryExpression:
					if propertyLookup, isPropertyLookup := asPropertyLookup(typedInnerOperand); isPropertyLookup {
						// Attempt to figure out the cast by looking at the left operand
						if leftHint, err := InferExpressionType(newExpression.LOperand); err != nil {
							return err
						} else if leftArrayHint, err := leftHint.ToArrayType(); err != nil {
							return err
						} else {
							newExpression.ROperand = pgsql.NewAnyExpression(
								pgsql.NewTypeCast(pgsql.ArrayExpression{
									Expression: pgsql.Select{
										Projection: []pgsql.Projection{pgsql.FunctionCall{
											Function: pgsql.FunctionJSONBArrayElementsText,
											Parameters: []pgsql.Expression{
												propertyLookup,
											},
											CastType: leftHint,
										}},
									},
								}, leftArrayHint),
							)
						}
					}
				}

			case pgsql.TypeHinted:
				newExpression.Operator = pgsql.OperatorEquals
				newExpression.ROperand = pgsql.AnyExpression{
					Expression: newExpression.ROperand,
					CastType:   typedROperand.TypeHint(),
				}

			default:
				// Attempt to figure out the cast by looking at the left operand
				if leftHint, err := InferExpressionType(newExpression.LOperand); err != nil {
					return err
				} else {
					newExpression.ROperand = pgsql.AnyExpression{
						Expression: newExpression.ROperand,
						CastType:   leftHint,
					}
				}

			}

			s.Push(newExpression)

		default:
			s.Push(newExpression)
		}

		return nil
	}
}

func (s *ExpressionTreeTranslator) PushOperator(operator pgsql.Operator) {
	// Track this operator for expression tree extraction
	switch operator {
	case pgsql.OperatorAnd:
		s.conjunctionDepth += 1

	case pgsql.OperatorOr:
		s.disjunctionDepth += 1
	}
}

func (s *ExpressionTreeTranslator) PopPushOperator(operator pgsql.Operator) error {
	// Track this operator for expression tree extraction and look to see if it's a candidate for rewriting
	switch operator {
	case pgsql.OperatorAnd:
		if s.disjunctionDepth == 0 {
			return s.ConstrainConjoinedOperandPair()
		}

		s.conjunctionDepth -= 1

	case pgsql.OperatorOr:
		if s.conjunctionDepth == 0 {
			return s.ConstrainDisjointOperandPair()
		}

		s.disjunctionDepth -= 1
	}

	return s.PopPushBinaryExpression(operator)
}
