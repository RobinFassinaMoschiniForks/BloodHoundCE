package translate

import (
	"fmt"
	"github.com/specterops/bloodhound/cypher/models"
	"github.com/specterops/bloodhound/cypher/models/pgsql"
	"strconv"
)

type IdentifierGenerator map[pgsql.DataType]int

func (s IdentifierGenerator) NewIdentifier(dataType pgsql.DataType) (pgsql.Identifier, error) {
	var (
		nextID    = s[dataType]
		nextIDStr = strconv.Itoa(nextID)
	)

	// Increment the ID
	s[dataType] = nextID + 1

	switch dataType {
	case pgsql.ExpansionPattern:
		return pgsql.Identifier("ex" + nextIDStr), nil
	case pgsql.PathComposite:
		return pgsql.Identifier("p" + nextIDStr), nil
	case pgsql.NodeComposite:
		return pgsql.Identifier("n" + nextIDStr), nil
	case pgsql.EdgeComposite:
		return pgsql.Identifier("e" + nextIDStr), nil
	case pgsql.NodeUpdateResult:
		return pgsql.Identifier("un" + nextIDStr), nil
	case pgsql.EdgeUpdateResult:
		return pgsql.Identifier("eu" + nextIDStr), nil
	case pgsql.ParameterIdentifier:
		return pgsql.Identifier("@p" + nextIDStr), nil
	default:
		return "", fmt.Errorf("identifier with data type %s does not have a prefix case", dataType)
	}
}

func NewIdentifierGenerator() IdentifierGenerator {
	return IdentifierGenerator{}
}

type Constraint struct {
	Dependencies *pgsql.IdentifierSet
	Expression   pgsql.Expression
}

/*
ConstraintTracker is a tool for associating constraints (e.g. binary or unary expressions
that constrain a set of identifiers) with the identifier set they constrain.

This is useful for rewriting a where-clause so that conjoined components can be isolated:

Where Clause:

where a.name = 'a' and b.name = 'b' and c.name = 'c' and a.num_a > 1 and a.ef = b.ef + c.ef

Isolated Constraints:

	"a":           a.name = 'a' and a.num_a > 1
	"b":           b.name = 'b'
	"c":           c.name = 'c'
	"a", "b", "c": a.ef = b.ef + c.ef
*/
type ConstraintTracker struct {
	Constraints []*Constraint
}

func NewConstraintTracker() *ConstraintTracker {
	return &ConstraintTracker{}
}

func (s *ConstraintTracker) ConsumeAll() (Constraint, error) {
	var (
		constraintExpressions = make([]pgsql.Expression, len(s.Constraints))
		matchedDependencies   = pgsql.NewIdentifierSet()
	)

	for idx, constraint := range s.Constraints {
		constraintExpressions[idx] = constraint.Expression
		matchedDependencies.MergeSet(constraint.Dependencies)
	}

	// Clear the internal constraint slice
	s.Constraints = s.Constraints[:0]

	if conjoined, err := ConjoinExpressions(constraintExpressions); err != nil {
		return Constraint{}, err
	} else {
		return Constraint{
			Dependencies: matchedDependencies,
			Expression:   conjoined,
		}, nil
	}
}

/*
ConsumeSet takes a given scope (a set of identifiers considered in-scope) and locates all constraints that can
be satisfied by the scope's identifiers.

```

	scope := pgsql.IdentifierSet{
		"a": struct{}{},
		"b": struct{}{},
	}

	tracker := ConstraintTracker{
		Constraints: []*Constraint{{
			Dependencies: pgsql.IdentifierSet{
				"a": struct{}{},
			},
			Expression: &pgsql.BinaryExpression{
				Operator: pgsql.OperatorEquals,
				LOperand: pgsql.CompoundIdentifier{"a", "name"},
				ROperand: pgsql.Literal{
					Value: "a",
				},
			},
		}},
	}

	satisfiedScope, expression := tracker.ConsumeSet(scope)

```
*/
func (s *ConstraintTracker) ConsumeSet(scope *pgsql.IdentifierSet) (Constraint, error) {
	var (
		matchedDependencies   = pgsql.NewIdentifierSet()
		constraintExpressions []pgsql.Expression
	)

	for idx := 0; idx < len(s.Constraints); {
		nextConstraint := s.Constraints[idx]

		if scope.Satisfies(nextConstraint.Dependencies) {
			// Remove this constraint
			s.Constraints = append(s.Constraints[:idx], s.Constraints[idx+1:]...)

			// Append the constraint as a conjoined expression
			constraintExpressions = append(constraintExpressions, nextConstraint.Expression)

			// Track which identifiers were satisfied
			matchedDependencies.MergeSet(nextConstraint.Dependencies)
		} else {
			// This constraint isn't satisfied by the identifiers in scope move to the next constraint
			idx += 1
		}
	}

	if conjoined, err := ConjoinExpressions(constraintExpressions); err != nil {
		return Constraint{}, err
	} else {
		return Constraint{
			Dependencies: matchedDependencies,
			Expression:   conjoined,
		}, nil
	}
}

func (s *ConstraintTracker) Constrain(dependencies *pgsql.IdentifierSet, constraintExpression pgsql.Expression) error {
	for _, constraint := range s.Constraints {
		if constraint.Dependencies.Matches(dependencies) {
			constraint.Expression = pgsql.OptionalAnd(constraint.Expression, constraintExpression)

			// TODO: Type negotiation
			return applyBinaryExpressionTypeHints(constraint.Expression.(*pgsql.BinaryExpression))
		}
	}

	s.Constraints = append(s.Constraints, &Constraint{
		Dependencies: dependencies,
		Expression:   constraintExpression,
	})
	return nil
}

type Provision struct {
	Joins     *pgsql.IdentifierSet
	Source    *pgsql.FromClause
	Reference *pgsql.FromClause
}

func (s Provision) Join(joinIdentifier pgsql.Identifier, joinClause pgsql.Join) {
	s.Joins.Add(joinIdentifier)
	s.Source.Joins = append(s.Source.Joins, joinClause)
}

type Scope struct {
	trunk               *Scope
	binding             *BoundIdentifier
	generator           IdentifierGenerator
	aliases             map[pgsql.Identifier]pgsql.Identifier
	bindings            map[pgsql.Identifier]*BoundIdentifier
	inScope             *pgsql.IdentifierSet
	resolvedIdentifiers *pgsql.IdentifierSet
}

func NewScope() *Scope {
	return &Scope{
		aliases:             map[pgsql.Identifier]pgsql.Identifier{},
		bindings:            map[pgsql.Identifier]*BoundIdentifier{},
		generator:           NewIdentifierGenerator(),
		inScope:             pgsql.NewIdentifierSet(),
		resolvedIdentifiers: pgsql.NewIdentifierSet(),
	}
}

func (s *Scope) SetScopeBinding(binding *BoundIdentifier) {
	for cursor := s; cursor != nil; cursor = s.trunk {
		cursor.binding = binding
	}
}

func (s *Scope) Binding() *BoundIdentifier {
	return s.binding
}

func (s *Scope) Descend() *Scope {
	// Descendent scopes receive only a copy of the resolved identifiers to allow for
	// scope binding to carry upstream while isolating use of generated from-clauses
	return &Scope{
		trunk:               s,
		binding:             s.binding,
		aliases:             s.aliases,
		bindings:            s.bindings,
		generator:           s.generator,
		inScope:             s.inScope,
		resolvedIdentifiers: s.resolvedIdentifiers.Copy(),
	}
}

func (s *Scope) Isolate() *Scope {
	// Isolated scopes receive a copy of the scoped and resolved identifiers to prevent
	// resolved identifiers from carrying upstream
	return &Scope{
		trunk:               s,
		binding:             s.binding,
		aliases:             s.aliases,
		bindings:            s.bindings,
		generator:           s.generator,
		inScope:             s.inScope.Copy(),
		resolvedIdentifiers: s.resolvedIdentifiers.Copy(),
	}
}

func (s *Scope) BuildProjection(identifier pgsql.Identifier) (pgsql.Projection, error) {
	return identifier, nil
}

func (s *Scope) BuildFromClauses(requiredIdentifiers ...pgsql.Identifier) ([]pgsql.FromClause, error) {
	var (
		fromClauses []pgsql.FromClause
	)

	if bindings, err := s.LookupBindings(requiredIdentifiers...); err != nil {
		return nil, err
	} else {
		for _, binding := range bindings {
			switch binding.DataType {
			case pgsql.NodeComposite:
				binding.Provision.Source = &pgsql.FromClause{
					Relation: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
						Binding: pgsql.AsOptionalIdentifier(binding.Identifier),
					},
				}

				binding.Provision.Reference = &pgsql.FromClause{
					Relation: pgsql.TableReference{
						Name: pgsql.CompoundIdentifier{binding.Identifier},
					},
				}

				fromClauses = append(fromClauses, *binding.Provision.Source)

			case pgsql.EdgeComposite:
				binding.Provision.Source = &pgsql.FromClause{
					Relation: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
						Binding: pgsql.AsOptionalIdentifier(binding.Identifier),
					},
				}

				binding.Provision.Reference = &pgsql.FromClause{
					Relation: pgsql.TableReference{
						Name: pgsql.CompoundIdentifier{binding.Identifier},
					},
				}

				fromClauses = append(fromClauses, *binding.Provision.Source)

			case pgsql.PathComposite, pgsql.ExpansionPath:
			// Path types are provided as an aggregation of its dependencies and has no special form

			case pgsql.ExpansionPattern:
				binding.Provision.Source.Relation = pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{binding.Identifier},
				}

				fromClauses = append(fromClauses, *binding.Provision.Source)

			case pgsql.ExpansionEdge:
				binding.Provision.Source = &pgsql.FromClause{
					Relation: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
						Binding: pgsql.AsOptionalIdentifier(binding.Identifier),
					},
				}

				binding.Provision.Reference = &pgsql.FromClause{
					Relation: pgsql.TableReference{
						Name: pgsql.CompoundIdentifier{binding.Identifier},
					},
				}

				fromClauses = append(fromClauses, *binding.Provision.Source)

			case pgsql.ExpansionRootNode, pgsql.ExpansionTerminalNode:
				binding.Provision.Source = &pgsql.FromClause{
					Relation: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
						Binding: pgsql.AsOptionalIdentifier(binding.Identifier),
					},
				}

				binding.Provision.Reference = &pgsql.FromClause{
					Relation: pgsql.TableReference{
						Name: pgsql.CompoundIdentifier{binding.Identifier},
					},
				}

				fromClauses = append(fromClauses, *binding.Provision.Source)

			default:
				return nil, fmt.Errorf("unknown from clause type: %s", binding.DataType)
			}
		}
	}

	return fromClauses, nil
}

func (s *Scope) Visible() *pgsql.IdentifierSet {
	return s.inScope.Copy()
}

func (s *Scope) lookupBinding(identifier pgsql.Identifier) (*BoundIdentifier, bool) {
	binding, hasBinding := s.bindings[identifier]
	return binding, hasBinding
}

func (s *Scope) LookupBindings(identifiers ...pgsql.Identifier) ([]*BoundIdentifier, error) {
	bindings := make([]*BoundIdentifier, len(identifiers))

	for idx, identifier := range identifiers {
		if binding, bound := s.bindings[identifier]; !bound {
			return nil, fmt.Errorf("missing bound identifier: %s", identifier)
		} else {
			bindings[idx] = binding
		}
	}

	return bindings, nil
}

func (s *Scope) Alias(alias pgsql.Identifier, binding *BoundIdentifier) {
	binding.Alias = models.ValueOptional(alias)
	s.aliases[alias] = binding.Identifier
}

func (s *Scope) Declare(identifier pgsql.Identifier) bool {
	if binding, bound := s.lookupBinding(identifier); bound {
		s.inScope.Add(identifier)

		binding.Provision = &Provision{
			Joins:  pgsql.NewIdentifierSet(),
			Source: &pgsql.FromClause{},
		}

		return true
	}

	return false
}

func (s *Scope) Retire(identifier pgsql.Identifier) bool {
	if _, bound := s.lookupBinding(identifier); bound {
		s.inScope.Remove(identifier)
		return true
	}

	return false
}

func (s *Scope) DefineNew(dataType pgsql.DataType) (*BoundIdentifier, error) {
	if newIdentifier, err := s.generator.NewIdentifier(dataType); err != nil {
		return nil, err
	} else {
		return s.Bind(newIdentifier, dataType), nil
	}
}

func (s *Scope) Lookup(identifier pgsql.Identifier) (*BoundIdentifier, bool) {
	if alias, aliased := s.aliases[identifier]; aliased {
		return s.lookupBinding(alias)
	} else {
		return s.lookupBinding(identifier)
	}
}

func (s *Scope) LookupString(identifierString string) (*BoundIdentifier, bool) {
	return s.Lookup(pgsql.Identifier(identifierString))
}

func (s *Scope) Bind(identifier pgsql.Identifier, dataType pgsql.DataType) *BoundIdentifier {
	boundIdentifier := &BoundIdentifier{
		Identifier: identifier,
		DataType:   dataType,
	}

	s.bindings[identifier] = boundIdentifier
	return boundIdentifier
}

func (s *Scope) Retype(identifier pgsql.Identifier, dataType pgsql.DataType) bool {
	if binding, bound := s.lookupBinding(identifier); bound {
		binding.DataType = dataType
		return true
	}

	return false
}

type BoundIdentifier struct {
	Identifier   pgsql.Identifier
	Provision    *Provision
	Alias        models.Optional[pgsql.Identifier]
	Parameter    models.Optional[*pgsql.Parameter]
	Dependencies []*BoundIdentifier
	ScopeBinding *BoundIdentifier
	DataType     pgsql.DataType
}

func (s *BoundIdentifier) DependOn(other *BoundIdentifier) {
	s.Dependencies = append(s.Dependencies, other)
}

func (s *BoundIdentifier) Link(other *BoundIdentifier) {
	s.DependOn(other)
	other.DependOn(s)
}

func (s *BoundIdentifier) RequiredScope() *pgsql.IdentifierSet {
	var (
		stack    = []*BoundIdentifier{s}
		resolved = pgsql.NewIdentifierSet()
	)

	for len(stack) > 0 {
		next := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		resolved.Add(next.Identifier)

		for _, dependency := range next.Dependencies {
			if !resolved.Contains(dependency.Identifier) {
				stack = append(stack, dependency)
			}
		}
	}

	return resolved
}

func (s *BoundIdentifier) FirstDependencyByType(dataType pgsql.DataType) (*BoundIdentifier, bool) {
	for _, dependency := range s.Dependencies {
		if dependency.DataType == dataType {
			return dependency, true
		}
	}

	return nil, false
}

func (s *BoundIdentifier) buildProjectionExpression(scope *Scope) (pgsql.Projection, error) {
	switch s.DataType {
	case pgsql.ExpansionEdge:
	case pgsql.ExpansionRootNode, pgsql.ExpansionTerminalNode:

	case pgsql.NodeComposite, pgsql.EdgeComposite:
		return pgsql.CompoundIdentifier{s.ScopeBinding.Identifier, s.Identifier}, nil

	case pgsql.PathComposite:
		var (
			nodeReferences = pgsql.ArrayLiteral{}
			edgeReferences = pgsql.ArrayLiteral{}
		)

		for _, dependency := range s.Dependencies {
			if compositeValue, err := dependency.buildProjectionExpression(scope); err != nil {
				return compositeValue, err
			} else {
				switch dependency.DataType {
				case pgsql.NodeComposite:
					nodeReferences.Values = append(nodeReferences.Values, compositeValue)

				case pgsql.EdgeComposite:
					edgeReferences.Values = append(edgeReferences.Values, compositeValue)

				default:
					return pgsql.CompositeValue{}, fmt.Errorf("unsupported nested composite type for pathcomposite: %s", s.DataType)
				}
			}
		}

		return pgsql.CompositeValue{
			Values: []pgsql.Expression{
				nodeReferences,
				edgeReferences,
			},
			DataType: pgsql.PathComposite,
		}, nil
	}

	return s.Identifier, nil
}

func (s *BoundIdentifier) BuildProjection(scope *Scope) (pgsql.Projection, error) {
	if compositeValue, err := s.buildProjectionExpression(scope); err != nil {
		return nil, err
	} else {
		return s.AliasProjection(compositeValue), nil
	}
}

func (s *BoundIdentifier) AliasExpression(expression pgsql.Expression) pgsql.Expression {
	if s.Alias.Set {
		return pgsql.AliasedExpression{
			Expression: expression,
			Alias:      s.Alias,
		}
	}

	return expression
}

func (s *BoundIdentifier) AliasProjection(projection pgsql.Projection) pgsql.Projection {
	if s.Alias.Set {
		return pgsql.AliasedExpression{
			Expression: projection,
			Alias:      s.Alias,
		}
	}

	return projection
}

type IdentifierTracker struct {
	aliases            map[string]pgsql.Identifier
	trackedIdentifiers map[pgsql.Identifier]*BoundIdentifier
}

func NewIdentifierTracker() *IdentifierTracker {
	return &IdentifierTracker{
		aliases:            map[string]pgsql.Identifier{},
		trackedIdentifiers: map[pgsql.Identifier]*BoundIdentifier{},
	}
}

func (s *IdentifierTracker) SetType(identifier pgsql.Identifier, dataType pgsql.DataType) error {
	if trackedIdentifier, isTracked := s.trackedIdentifiers[identifier]; !isTracked {
		return fmt.Errorf("unknown identifier: '%s'", identifier)
	} else {
		trackedIdentifier.DataType = dataType
		return nil
	}
}

func (s *IdentifierTracker) DependsOn(identifier pgsql.Identifier, dependencies ...pgsql.Identifier) error {
	if trackedIdentifier, isTracked := s.trackedIdentifiers[identifier]; !isTracked {
		return fmt.Errorf("unknown identifier: %s", identifier)
	} else {
		for _, dependency := range dependencies {
			if trackedDependency, isTracked := s.trackedIdentifiers[dependency]; !isTracked {
				return fmt.Errorf("unknown dependent identifier: %s", dependency)
			} else {
				trackedIdentifier.Dependencies = append(trackedIdentifier.Dependencies, trackedDependency)
			}
		}
	}

	return nil
}

func (s *IdentifierTracker) Track(identifier pgsql.Identifier, dataType pgsql.DataType) *BoundIdentifier {
	newTrackedIdentifier := &BoundIdentifier{
		Identifier: identifier,
		DataType:   dataType,
	}

	s.aliases[identifier.String()] = identifier
	s.trackedIdentifiers[identifier] = newTrackedIdentifier

	return newTrackedIdentifier
}

func (s *IdentifierTracker) Alias(oldIdentifier string, identifier pgsql.Identifier, dataType pgsql.DataType) {
	s.aliases[oldIdentifier] = identifier

	newTrackedIdentifier := s.Track(identifier, dataType)
	newTrackedIdentifier.Alias = models.ValueOptional(pgsql.Identifier(oldIdentifier))
}

func (s *IdentifierTracker) TrackString(identifier string, dataType pgsql.DataType) {
	s.Track(pgsql.Identifier(identifier), dataType)
}

func (s *IdentifierTracker) Lookup(identifier pgsql.Identifier) (*BoundIdentifier, bool) {
	trackedIdentifier, found := s.trackedIdentifiers[identifier]
	return trackedIdentifier, found
}

func (s *IdentifierTracker) LookupAlias(oldIdentifier string) (pgsql.Identifier, bool) {
	alias, found := s.aliases[oldIdentifier]
	return alias, found
}
