package translate

import (
	"fmt"
	cypher "github.com/specterops/bloodhound/cypher/model"
	"github.com/specterops/bloodhound/cypher/models"
	"github.com/specterops/bloodhound/cypher/models/pgsql"
	"github.com/specterops/bloodhound/dawgs/graph"
)

const (
	expansionRootID     pgsql.Identifier = "root_id"
	nextExpansionNodeID pgsql.Identifier = "next_id"
	expansionDepth      pgsql.Identifier = "depth"
	expansionSatisfied  pgsql.Identifier = "satisfied"
	expansionIsCycle    pgsql.Identifier = "is_cycle"
	expansionPath       pgsql.Identifier = "path"
)

func expansionColumns() pgsql.RowShape {
	return pgsql.RowShape{
		Columns: []pgsql.Identifier{
			expansionRootID,
			nextExpansionNodeID,
			expansionDepth,
			expansionSatisfied,
			expansionIsCycle,
			expansionPath,
		},
	}
}

type Match struct {
	Scope *Scope
}

type NodeSelect struct {
	Identifier models.Optional[pgsql.Identifier]
}

type Expansion struct {
	Identifier pgsql.Identifier
	MinDepth   models.Optional[int64]
	MaxDepth   models.Optional[int64]
}

type TraversalStep struct {
	Direction           graph.Direction
	Expansion           models.Optional[Expansion]
	LeftNodeIdentifier  models.Optional[pgsql.Identifier]
	EdgeIdentifier      models.Optional[pgsql.Identifier]
	RightNodeIdentifier models.Optional[pgsql.Identifier]
}

type PatternPart struct {
	IsTraversal    bool
	PatternBinding models.Optional[*BoundIdentifier]
	TraversalSteps []*TraversalStep
	NodeSelect     NodeSelect
}

func (s *PatternPart) ContainsExpansions() bool {
	for _, traversalStep := range s.TraversalSteps {
		if traversalStep.Expansion.Set {
			return true
		}
	}

	return false
}

type Pattern struct {
	Parts []*PatternPart
}

func (s *Pattern) Reset() {
	s.Parts = s.Parts[:0]
}

func (s *Pattern) NewPart() *PatternPart {
	newPatternPart := &PatternPart{}

	s.Parts = append(s.Parts, newPatternPart)
	return newPatternPart
}

func (s *Pattern) CurrentPart() *PatternPart {
	return s.Parts[len(s.Parts)-1]
}

type Query struct {
	Scope   *Scope
	Updates []*Update
	OrderBy []pgsql.OrderBy
	Skip    models.Optional[pgsql.Expression]
	Limit   models.Optional[pgsql.Expression]
}

func (s *Query) CurrentOrderBy() *pgsql.OrderBy {
	return &s.OrderBy[len(s.OrderBy)-1]
}

type Where struct{}

func NewWhere() *Where {
	return &Where{}
}

type Projection struct {
	Expression pgsql.Expression
	Alias      models.Optional[pgsql.Identifier]
}

func (s *Projection) SetIdentifier(identifier pgsql.Identifier) {
	s.Expression = identifier
}

func (s *Projection) SetAlias(alias pgsql.Identifier) {
	s.Alias = models.ValueOptional(alias)
}

type Assignment struct {
	Binding    *BoundIdentifier
	Target     *BoundIdentifier
	Expression *pgsql.BinaryExpression
}

type Update struct {
	Assignments []*Assignment
}

func (s *Update) NewAssignment(scope *Scope, targetIdentifier pgsql.Identifier) (*Assignment, error) {
	if targetBinding, bound := scope.Lookup(targetIdentifier); !bound {
		return nil, fmt.Errorf("invalid identifier: %s", targetIdentifier)
	} else if updateResultType, err := targetBinding.DataType.ToUpdateResultType(); err != nil {
		return nil, fmt.Errorf("invalid data type for update: %s", targetBinding.DataType)
	} else if innerBinding, err := scope.DefineNew(updateResultType); err != nil {
		return nil, err
	} else {
		// Link the update to the target it's updating
		innerBinding.Link(targetBinding)

		newUpdate := &Assignment{
			Binding: innerBinding,
			Target:  targetBinding,
		}

		s.Assignments = append(s.Assignments, newUpdate)
		return newUpdate, nil
	}
}

type ProjectionClause struct {
	Distinct    bool
	Projections []*Projection
}

func NewProjectionClause() *ProjectionClause {
	return &ProjectionClause{}
}

func (s *ProjectionClause) PushProjection() {
	s.Projections = append(s.Projections, &Projection{})
}

func (s *ProjectionClause) CurrentProjection() *Projection {
	return s.Projections[len(s.Projections)-1]
}

func extractIdentifierFromCypherExpression(expression cypher.Expression) (pgsql.Identifier, bool, error) {
	if expression == nil {
		return "", false, nil
	}

	var variableExpression cypher.Expression

	switch typedExpression := expression.(type) {
	case *cypher.NodePattern:
		variableExpression = typedExpression.Binding

	case *cypher.RelationshipPattern:
		variableExpression = typedExpression.Binding

	case *cypher.PatternPart:
		variableExpression = typedExpression.Binding

	case *cypher.ProjectionItem:
		variableExpression = typedExpression.Binding

	case *cypher.Variable:
		variableExpression = typedExpression

	default:
		return "", false, fmt.Errorf("unable to extract variable from expression type: %T", expression)
	}

	if variableExpression == nil {
		return "", false, nil
	}

	switch typedVariableExpression := variableExpression.(type) {
	case *cypher.Variable:
		return pgsql.Identifier(typedVariableExpression.Symbol), true, nil

	default:
		return "", false, fmt.Errorf("unknown variable expression type: %T", variableExpression)
	}
}

func nodeJoinColumnsForPatternDirection(direction graph.Direction) (pgsql.Identifier, pgsql.Identifier, error) {
	switch direction {
	case graph.DirectionOutbound:
		return pgsql.ColumnStartID, pgsql.ColumnEndID, nil

	case graph.DirectionInbound:
		return pgsql.ColumnEndID, pgsql.ColumnStartID, nil

	default:
		return "", "", fmt.Errorf("unsupported direction: %d", direction)
	}
}
