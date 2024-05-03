package translate

import (
	"fmt"
	cypher "github.com/specterops/bloodhound/cypher/model"
	"github.com/specterops/bloodhound/cypher/models"
	"github.com/specterops/bloodhound/cypher/models/pgsql"
	"github.com/specterops/bloodhound/cypher/models/tree"
)

type State int

const (
	StateTranslatingStart State = iota
	StateTranslatingPatternPart
	StateTranslatingMatch
	StateTranslatingWhere
	StateTranslatingProjection
	StateTranslatingOrderBy
	StateTranslatingUpdate
	StateTranslatingNestedExpression
)

func (s State) String() string {
	switch s {
	case StateTranslatingStart:
		return "start"
	case StateTranslatingPatternPart:
		return "pattern part"
	case StateTranslatingMatch:
		return "match clause"
	case StateTranslatingWhere:
		return "where clause"
	case StateTranslatingProjection:
		return "projection"
	case StateTranslatingOrderBy:
		return "order by"
	case StateTranslatingNestedExpression:
		return "nested expression"
	default:
		return ""
	}
}

type Translator struct {
	tree.HierarchicalVisitor[cypher.SyntaxNode]

	kindMapper        pgsql.KindMapper
	translatedQuery   *pgsql.Query
	translatedQueries []*pgsql.Query
	state             []State
	treeTranslator    *ExpressionTreeTranslator

	pattern     *Pattern
	where       *Where
	match       *Match
	projections *ProjectionClause
	update      *Update
	query       *Query
}

func NewTranslator(kindMapper pgsql.KindMapper, panicOnErr bool) *Translator {
	return &Translator{
		HierarchicalVisitor: tree.NewComposableHierarchicalVisitor[cypher.SyntaxNode](panicOnErr),
		kindMapper:          kindMapper,
		treeTranslator:      NewExpressionTreeTranslator(),
		pattern:             &Pattern{},
	}
}

func (s *Translator) TranslatedStatements() []TranslatedStatement {
	translated := make([]TranslatedStatement, len(s.translatedQueries))

	for i, query := range s.translatedQueries {
		translated[i] = TranslatedStatement{
			Statement:  *query,
			Parameters: nil,
		}
	}

	return translated
}

func (s *Translator) currentState() State {
	return s.state[len(s.state)-1]
}

func (s *Translator) pushState(state State) {
	s.state = append(s.state, state)
}

func (s *Translator) popState() {
	s.state = s.state[:len(s.state)-1]
}

func (s *Translator) exitState(expectedState State) {
	if currentState := s.currentState(); currentState != expectedState {
		s.SetErrorf("expected state %s but found %s", expectedState, currentState)
	} else {
		s.state = s.state[:len(s.state)-1]
	}
}

func (s *Translator) inState(expectedState State) bool {
	for _, state := range s.state {
		if state == expectedState {
			return true
		}
	}

	return false
}

func (s *Translator) Enter(expression cypher.SyntaxNode) {
	switch typedExpression := expression.(type) {
	case *cypher.RegularQuery, *cypher.SingleQuery, *cypher.PatternElement, *cypher.Return,
		*cypher.Comparison, *cypher.Skip, *cypher.Limit, cypher.Operator, *cypher.ArithmeticExpression:
	// No operation for these syntax nodes

	case *cypher.Negation:
		s.pushState(StateTranslatingNestedExpression)

	case *cypher.MultiPartQueryPart:
		s.SetErrorf("unsupported query operation: %T", expression)

	case *cypher.SinglePartQuery:
		s.translatedQuery = &pgsql.Query{
			CommonTableExpressions: &pgsql.With{},
		}

		s.query = &Query{
			Scope: NewScope(),
		}

	case *cypher.ReadingClause:
	case *cypher.Match:
		s.pushState(StateTranslatingMatch)

		// Start with a fresh match and where clause. Instantiation of the where clause here is necessary since
		// cypher will store identifier constraints in the query pattern which precedes the query where clause.
		s.match = &Match{
			Scope: s.query.Scope,
		}

		// Assign a new where clause and clear out any lingering pattern translations
		s.where = NewWhere()
		s.pattern.Reset()

	case *cypher.Where:
		// Track that we're in a where clause first
		s.pushState(StateTranslatingWhere)

		// If there's a where AST node present in the cypher model we likely have an expression to translate
		s.pushState(StateTranslatingNestedExpression)

	case *cypher.KindMatcher:
		if err := s.translateKindMatcher(typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.Parameter:
		var (
			cypherIdentifier = pgsql.Identifier(typedExpression.Symbol)
			binding          *BoundIdentifier
		)

		if existingBinding, bound := s.query.Scope.Lookup(cypherIdentifier); bound {
			// Set the outer reference
			binding = existingBinding
		} else {
			if newBinding, err := s.query.Scope.DefineNew(pgsql.ParameterIdentifier); err != nil {
				s.SetError(err)
			} else {
				// Alias the old parameter identifier to the synthetic one
				s.query.Scope.Alias(cypherIdentifier, newBinding)

				// Create a new container for the parameter and its value
				if newParameter, err := pgsql.AsParameter(newBinding.Identifier, typedExpression.Value); err != nil {
					s.SetError(err)
				} else {
					newBinding.Parameter = models.ValueOptional(newParameter)
				}

				// Set the outer reference
				binding = newBinding
			}
		}

		switch currentState := s.currentState(); currentState {
		case StateTranslatingNestedExpression:
			s.treeTranslator.Push(binding.Parameter.Value)

		default:
			s.SetErrorf("invalid state \"%s\" for cypher AST node %T", s.currentState(), expression)
		}

	case *cypher.Variable:
		switch currentState := s.currentState(); currentState {
		case StateTranslatingNestedExpression:
			if binding, resolved := s.query.Scope.LookupString(typedExpression.Symbol); !resolved {
				s.SetErrorf("unable to find identifier %s", typedExpression.Symbol)
			} else {
				s.treeTranslator.Push(binding.Identifier)
			}

		default:
			s.SetErrorf("invalid state \"%s\" for cypher AST node %T", s.currentState(), expression)
		}

	case *cypher.ListLiteral:
		s.pushState(StateTranslatingNestedExpression)

	case *cypher.Literal:
		var literal pgsql.Literal

		if strLiteral, isStr := typedExpression.Value.(string); isStr {
			// Cypher parser wraps string literals with ' characters - unwrap them first
			literal = pgsql.MustAsLiteral(strLiteral[1 : len(strLiteral)-1])
			literal.Null = typedExpression.Null
		} else {
			literal = pgsql.MustAsLiteral(typedExpression.Value)
			literal.Null = typedExpression.Null
		}

		// If the literal isn't null then attempt to negotiate its type
		if !literal.Null {
			if literalType, err := pgsql.ValueToDataType(literal.Value); err != nil {
				s.SetError(err)
			} else {
				literal.CastType = literalType
			}
		}

		switch currentState := s.currentState(); currentState {
		case StateTranslatingNestedExpression:
			s.treeTranslator.Push(literal)

		default:
			s.SetErrorf("invalid state \"%s\" for cypher AST node %T", s.currentState(), expression)
		}

	case *cypher.Parenthetical:
		s.pushState(StateTranslatingNestedExpression)
		s.treeTranslator.Push(&pgsql.Parenthetical{})

	case *cypher.FunctionInvocation:
		s.pushState(StateTranslatingNestedExpression)

	case *cypher.PropertyLookup:
		if variable, isVariable := typedExpression.Atom.(*cypher.Variable); !isVariable {
			s.SetErrorf("expected variable for property lookup reference but found type: %T", typedExpression.Atom)
		} else if resolved, isResolved := s.query.Scope.LookupString(variable.Symbol); !isResolved {
			s.SetErrorf("unable to resolve identifier: %s", variable.Symbol)
		} else {
			switch currentState := s.currentState(); currentState {
			case StateTranslatingNestedExpression:
				// TODO: Cypher does not support nested property references so the Symbols slice should be a string
				if fieldIdentifierLiteral, err := pgsql.AsLiteral(typedExpression.Symbols[0]); err != nil {
					s.SetError(err)
				} else {
					s.treeTranslator.Push(pgsql.CompoundIdentifier{resolved.Identifier, pgsql.ColumnProperties})
					s.treeTranslator.Push(fieldIdentifierLiteral)
				}

			default:
				s.SetErrorf("invalid state \"%s\" for cypher AST node %T", s.currentState(), expression)
			}
		}

	case *cypher.Order:
		s.pushState(StateTranslatingOrderBy)

	case *cypher.SortItem:
		s.pushState(StateTranslatingNestedExpression)

		s.query.OrderBy = append(s.query.OrderBy, pgsql.OrderBy{
			Ascending: typedExpression.Ascending,
		})

	case *cypher.Projection:
		s.pushState(StateTranslatingProjection)

		if err := s.translateProjection(s.query.Scope.Isolate(), typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.ProjectionItem:
		s.pushState(StateTranslatingNestedExpression)

		s.projections.PushProjection()

		if variableSymbol, hasBinding, err := extractIdentifierFromCypherExpression(typedExpression); err != nil {
			s.SetError(err)
		} else if hasBinding {
			s.projections.CurrentProjection().SetAlias(variableSymbol)
		}

	case *cypher.PatternPart:
		s.pushState(StateTranslatingPatternPart)

		if err := s.translatePatternPart(s.match.Scope, typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.NodePattern:
		if err := s.translateNodePattern(s.match.Scope, typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.RelationshipPattern:
		if err := s.translateRelationshipPattern(s.match.Scope, typedExpression); err != nil {
			s.SetError(err)
		}

	case *cypher.PartialComparison:
		s.treeTranslator.PushOperator(pgsql.Operator(typedExpression.Operator))

	case *cypher.PartialArithmeticExpression:
		s.treeTranslator.PushOperator(pgsql.Operator(typedExpression.Operator))

	case *cypher.Disjunction:
		for idx := 0; idx < typedExpression.Len()-1; idx++ {
			s.treeTranslator.PushOperator(pgsql.OperatorOr)
		}

	case *cypher.Conjunction:
		for idx := 0; idx < typedExpression.Len()-1; idx++ {
			s.treeTranslator.PushOperator(pgsql.OperatorAnd)
		}

	case *cypher.UpdatingClause:
		s.pushState(StateTranslatingUpdate)
		s.update = &Update{}

	case *cypher.Set:
	case *cypher.SetItem:
		s.pushState(StateTranslatingNestedExpression)

	default:
		s.SetErrorf("unable to translate cypher type: %T", expression)
	}
}

func (s *Translator) Exit(expression cypher.SyntaxNode) {
	switch typedExpression := expression.(type) {

	case *cypher.SetItem:
		s.exitState(StateTranslatingNestedExpression)

		if operator, err := translateCypherAssignmentOperator(typedExpression.Operator); err != nil {
			s.SetError(err)
		} else if err := s.translateSetOperation(s.query.Scope, operator); err != nil {
			s.SetError(err)
		}

	case *cypher.UpdatingClause:
		s.exitState(StateTranslatingUpdate)

		if err := s.buildUpdates(s.query.Scope); err != nil {
			s.SetError(err)
		}

	case *cypher.ListLiteral:
		s.exitState(StateTranslatingNestedExpression)

		var (
			numExpressions = len(typedExpression.Expressions())
			literal        = pgsql.ArrayLiteral{
				Values:   make([]pgsql.Expression, numExpressions),
				CastType: pgsql.UnsetDataType,
			}
		)

		for idx := numExpressions - 1; idx >= 0; idx-- {
			if nextExpression, err := s.treeTranslator.Pop(); err != nil {
				s.SetError(err)
			} else {
				if typeHint, isTypeHinted := nextExpression.(pgsql.TypeHinted); isTypeHinted {
					if literal.CastType != pgsql.UnsetDataType {
						if nextExpressionType := typeHint.TypeHint(); literal.CastType != nextExpressionType {
							s.SetErrorf("expected array literal value type %s at index %d but found type %s", literal.CastType, idx, nextExpressionType)
						}
					} else {
						literal.CastType = typeHint.TypeHint()
					}
				}

				literal.Values[idx] = nextExpression
			}
		}

		if literal.CastType == pgsql.UnsetDataType {
			s.SetErrorf("array literal has no available type hints")
		} else {
			s.treeTranslator.Push(literal)
		}

	case *cypher.Order:
		s.exitState(StateTranslatingOrderBy)

	case *cypher.SortItem:
		s.exitState(StateTranslatingNestedExpression)

		if lookupExpression, err := s.treeTranslator.Pop(); err != nil {
			s.SetError(err)
		} else {
			// Make sure to write all property lookup operators in order by statements as ->
			if propertyLookup, isPropertyLookup := asPropertyLookup(lookupExpression); isPropertyLookup {
				propertyLookup.Operator = pgsql.OperatorJSONField
			}

			currentScopeBinding := s.query.Scope.Binding()

			if err := RewriteExpressionCompoundIdentifiers(lookupExpression, s.query.Scope.Visible(), func(identifier pgsql.CompoundIdentifier) (pgsql.Expression, error) {
				return CompositeTypeFieldLookup(currentScopeBinding, identifier), nil
			}); err != nil {
				s.SetError(err)
			} else {
				s.query.CurrentOrderBy().Expression = lookupExpression
			}
		}

	case *cypher.KindMatcher:
		switch currentState := s.currentState(); currentState {
		case StateTranslatingNestedExpression:
			if matcher, err := s.treeTranslator.Pop(); err != nil {
				s.SetError(err)
			} else {
				s.treeTranslator.Push(matcher)
			}

		default:
			s.SetErrorf("invalid state \"%s\" for cypher AST node %T", s.currentState(), expression)
		}

	case *cypher.Parenthetical:
		s.exitState(StateTranslatingNestedExpression)

		// Pull the sub-expression we wrap
		if wrappedExpression, err := s.treeTranslator.Pop(); err != nil {
			s.SetError(err)
		} else if parenthetical, err := PopFromBuilderAs[*pgsql.Parenthetical](s.treeTranslator); err != nil {
			s.SetError(err)
		} else {
			parenthetical.Expression = wrappedExpression

			switch currentState := s.currentState(); currentState {
			case StateTranslatingNestedExpression:
				s.treeTranslator.Push(*parenthetical)

			default:
				s.SetErrorf("invalid state \"%s\" for cypher AST node %T", s.currentState(), expression)
			}
		}

	case *cypher.FunctionInvocation:
		s.exitState(StateTranslatingNestedExpression)

		switch typedExpression.Name {
		case cypher.IdentityFunction:
			if referenceArgument, err := PopFromBuilderAs[pgsql.Identifier](s.treeTranslator); err != nil {
				s.SetError(err)
			} else {
				s.treeTranslator.Push(pgsql.CompoundIdentifier{referenceArgument, pgsql.ColumnID})
			}

		case cypher.LocalTimeFunction:
			if err := s.translateDateTimeFunctionCall(typedExpression, pgsql.TimeWithoutTimeZone); err != nil {
				s.SetError(err)
			}

		case cypher.LocalDateTimeFunction:
			if err := s.translateDateTimeFunctionCall(typedExpression, pgsql.TimestampWithoutTimeZone); err != nil {
				s.SetError(err)
			}

		case cypher.DateFunction:
			if err := s.translateDateTimeFunctionCall(typedExpression, pgsql.Date); err != nil {
				s.SetError(err)
			}

		case cypher.DateTimeFunction:
			if err := s.translateDateTimeFunctionCall(typedExpression, pgsql.TimestampWithTimeZone); err != nil {
				s.SetError(err)
			}

		case cypher.ToLowerFunction:
			if typedExpression.NumArguments() > 1 {
				s.SetError(fmt.Errorf("expected only one text argument for cypher function: %s", typedExpression.Name))
			} else if argument, err := s.treeTranslator.Pop(); err != nil {
				s.SetError(err)
			} else {
				if propertyLookup, isPropertyLookup := asPropertyLookup(argument); isPropertyLookup {
					// Rewrite the property lookup operator with a JSON text field lookup
					propertyLookup.Operator = pgsql.OperatorJSONTextField
				}

				s.treeTranslator.Push(pgsql.FunctionCall{
					Function:   pgsql.FunctionToLower,
					Parameters: []pgsql.Expression{argument},
					CastType:   pgsql.Text,
				})
			}

		default:
			s.SetErrorf("unknown cypher function: %s", typedExpression.Name)
		}

	case *cypher.Negation:
		s.exitState(StateTranslatingNestedExpression)

		switch currentState := s.currentState(); currentState {
		case StateTranslatingNestedExpression:
			if operand, err := s.treeTranslator.Pop(); err != nil {
				s.SetError(err)
			} else {
				s.treeTranslator.Push(&pgsql.UnaryExpression{
					Operator: pgsql.OperatorNot,
					Operand:  operand,
				})
			}

		default:
			s.SetErrorf("invalid state \"%s\" for cypher AST node %T", s.currentState(), expression)
		}

	case *cypher.PatternPart:
		s.exitState(StateTranslatingPatternPart)

	case *cypher.Where:
		// Validate state transitions
		s.exitState(StateTranslatingNestedExpression)
		s.exitState(StateTranslatingWhere)

		// Assign the last operands as identifier set constraints
		if err := s.treeTranslator.ConstrainRemainingOperands(); err != nil {
			s.SetError(err)
		}

	case *cypher.PropertyLookup:
		switch currentState := s.currentState(); currentState {
		case StateTranslatingNestedExpression:
			if err := s.treeTranslator.PopPushOperator(pgsql.OperatorPropertyLookup); err != nil {
				s.SetError(err)
			}

		case StateTranslatingProjection:
			if lookupExpression, err := s.treeTranslator.Pop(); err != nil {
				s.SetError(err)
			} else {
				s.projections.CurrentProjection().Expression = lookupExpression
			}

		default:
			s.SetErrorf("invalid state \"%s\" for cypher AST node %T", s.currentState(), expression)
		}

	case *cypher.PartialComparison:
		switch currentState := s.currentState(); currentState {
		case StateTranslatingNestedExpression:
			if err := s.treeTranslator.PopPushOperator(pgsql.Operator(typedExpression.Operator)); err != nil {
				s.SetError(err)
			}

		default:
			s.SetErrorf("invalid state \"%s\" for cypher AST node %T", s.currentState(), expression)
		}

	case *cypher.PartialArithmeticExpression:
		switch currentState := s.currentState(); currentState {
		case StateTranslatingNestedExpression:
			if err := s.treeTranslator.PopPushOperator(pgsql.Operator(typedExpression.Operator)); err != nil {
				s.SetError(err)
			}

		default:
			s.SetErrorf("invalid state \"%s\" for cypher AST node %T", s.currentState(), expression)
		}

	case *cypher.Disjunction:
		switch currentState := s.currentState(); currentState {
		case StateTranslatingNestedExpression:
			for idx := 0; idx < typedExpression.Len()-1; idx++ {
				if err := s.treeTranslator.PopPushOperator(pgsql.OperatorOr); err != nil {
					s.SetError(err)
				}
			}

		default:
			s.SetErrorf("invalid state \"%s\" for cypher AST node %T", s.currentState(), expression)
		}

	case *cypher.Conjunction:
		switch currentState := s.currentState(); currentState {
		case StateTranslatingNestedExpression:
			for idx := 0; idx < typedExpression.Len()-1; idx++ {
				if err := s.treeTranslator.PopPushOperator(pgsql.OperatorAnd); err != nil {
					s.SetError(err)
				}
			}

		default:
			s.SetErrorf("invalid state \"%s\" for cypher AST node %T", s.currentState(), expression)
		}

	case *cypher.ProjectionItem:
		s.exitState(StateTranslatingNestedExpression)

		if projectionExpression, err := s.treeTranslator.Pop(); err != nil {
			s.SetError(err)
		} else {
			// Make sure to write all property lookup operators in order by statements as ->
			if propertyLookup, isPropertyLookup := asPropertyLookup(projectionExpression); isPropertyLookup {
				propertyLookup.Operator = pgsql.OperatorJSONField
			}

			s.projections.CurrentProjection().Expression = projectionExpression
		}

	case *cypher.Projection:
		s.exitState(StateTranslatingProjection)

	case *cypher.Return:
		if err := s.buildProjection(s.query.Scope); err != nil {
			s.SetError(err)
		}

	case *cypher.Match:
		s.exitState(StateTranslatingMatch)

		if err := s.buildMatch(s.match.Scope); err != nil {
			s.SetError(err)
		}

	case *cypher.SinglePartQuery:
		// If there was no return specified end the CTE chain with a bare select
		if typedExpression.Return == nil {
			if literalReturn, err := pgsql.AsLiteral(1); err != nil {
				s.SetError(err)
			} else {
				s.translatedQuery.Body = pgsql.Select{
					Projection: []pgsql.Projection{literalReturn},
				}
			}
		}

		s.translatedQueries = append(s.translatedQueries, s.translatedQuery)
	}
}

type TranslatedStatement struct {
	Statement  pgsql.Statement
	Parameters map[string]any
}

func Translate(cypherQuery *cypher.RegularQuery, kindMapper pgsql.KindMapper, panicOnErr bool) ([]TranslatedStatement, error) {
	var (
		translator = NewTranslator(kindMapper, panicOnErr)
	)

	if err := tree.WalkCypher(cypherQuery, translator); err != nil {
		return nil, err
	}

	return translator.TranslatedStatements(), nil
}
