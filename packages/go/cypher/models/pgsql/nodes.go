package pgsql

import "fmt"

type SyntaxNode interface {
	NodeType() string
}

type Statement interface {
	SyntaxNode
	AsStatement() Statement
}

type Assignment interface {
	SyntaxNode
	AsAssignment() Assignment
}

type Expression interface {
	SyntaxNode
	AsExpression() Expression
}

type TypeHinted interface {
	Expression
	TypeHint() DataType
}

type Projection interface {
	Expression
	AsProjection() Projection
}

func As[T any](node SyntaxNode) (T, error) {
	var empty T

	if node == nil {
		return empty, nil
	}

	if projection, isT := node.(T); isT {
		return projection, nil
	}

	return empty, fmt.Errorf("node type %T does not convert to expected type %T", node, empty)
}

type MergeAction interface {
	Expression
	AsMergeAction() MergeAction
}

type SetExpression interface {
	Expression
	AsSetExpression() SetExpression
}

type ConflictAction interface {
	Expression
	AsConflictAction() ConflictAction
}
