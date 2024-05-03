package translate

import (
	"github.com/specterops/bloodhound/cypher/models/pgsql"
	"github.com/specterops/bloodhound/cypher/models/tree"
)

type CompoundIdentifierRewriter struct {
	tree.HierarchicalVisitor[pgsql.SyntaxNode]

	roots    *pgsql.IdentifierSet
	rewriter func(identifier pgsql.CompoundIdentifier) (pgsql.Expression, error)
}

func (s *CompoundIdentifierRewriter) enter(node pgsql.SyntaxNode) error {
	switch typedExpression := node.(type) {
	case pgsql.CompositeValue:
		for idx, value := range typedExpression.Values {
			switch typedValue := value.(type) {
			case pgsql.CompoundIdentifier:
				if s.roots.Contains(typedValue.Root()) {
					if rewritten, err := s.rewriter(typedValue); err != nil {
						return err
					} else {
						typedExpression.Values[idx] = rewritten
					}
				}
			}
		}

	case pgsql.FunctionCall:
		for idx, parameter := range typedExpression.Parameters {
			switch typedParameter := parameter.(type) {
			case pgsql.CompoundIdentifier:
				if s.roots.Contains(typedParameter.Root()) {
					if rewritten, err := s.rewriter(typedParameter); err != nil {
						return err
					} else {
						typedExpression.Parameters[idx] = rewritten
					}
				}
			}
		}

	case *pgsql.BinaryExpression:
		switch typedLOperand := typedExpression.LOperand.(type) {
		case pgsql.CompoundIdentifier:
			if s.roots.Contains(typedLOperand.Root()) {
				if rewritten, err := s.rewriter(typedLOperand); err != nil {
					return err
				} else {
					typedExpression.LOperand = rewritten
				}
			}
		}

		switch typedROperand := typedExpression.ROperand.(type) {
		case pgsql.CompoundIdentifier:
			if s.roots.Contains(typedROperand.Root()) {
				if rewritten, err := s.rewriter(typedROperand); err != nil {
					return err
				} else {
					typedExpression.LOperand = rewritten
				}
			}
		}
	}

	return nil
}

func (s *CompoundIdentifierRewriter) Enter(node pgsql.SyntaxNode) {
	if err := s.enter(node); err != nil {
		s.SetError(err)
	}
}

func NewCompoundIdentifierRewriter(roots *pgsql.IdentifierSet, rewriter func(identifier pgsql.CompoundIdentifier) (pgsql.Expression, error)) tree.HierarchicalVisitor[pgsql.SyntaxNode] {
	return &CompoundIdentifierRewriter{
		HierarchicalVisitor: tree.NewComposableHierarchicalVisitor[pgsql.SyntaxNode](false),
		roots:               roots,
		rewriter:            rewriter,
	}
}

func RewriteExpressionCompoundIdentifiers(expression pgsql.Expression, roots *pgsql.IdentifierSet, rewriter func(identifier pgsql.CompoundIdentifier) (pgsql.Expression, error)) error {
	if expression == nil {
		return nil
	}

	return tree.WalkPgSQL(expression, NewCompoundIdentifierRewriter(roots, rewriter))
}

func RewriteExpressionCompoundIdentifier(expression pgsql.Expression, root pgsql.Identifier, rewriter func(identifier pgsql.CompoundIdentifier) (pgsql.Expression, error)) error {
	return RewriteExpressionCompoundIdentifiers(expression, pgsql.AsIdentifierSet(root), rewriter)
}
