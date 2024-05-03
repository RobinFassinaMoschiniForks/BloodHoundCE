package tree

import (
	"fmt"
	"github.com/specterops/bloodhound/cypher/models/pgsql"
)

func newSQLWalkCursor(node pgsql.SyntaxNode) (*Cursor[pgsql.SyntaxNode], error) {
	switch typedNode := node.(type) {
	case pgsql.Query:
		nextCursor := &Cursor[pgsql.SyntaxNode]{
			Node: node,
		}

		if typedNode.CommonTableExpressions != nil {
			nextCursor.Branches = append(nextCursor.Branches, *typedNode.CommonTableExpressions)
		}

		nextCursor.Branches = append(nextCursor.Branches, typedNode.Body)
		return nextCursor, nil

	case pgsql.With:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: pgsql.MustSliceTypeConvert[pgsql.SyntaxNode](typedNode.Expressions),
		}, nil

	case pgsql.CommonTableExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Query, typedNode.Alias},
		}, nil

	case pgsql.Select:
		nextCursor := &Cursor[pgsql.SyntaxNode]{
			Node: node,
		}

		nextCursor.AddBranches(pgsql.MustSliceTypeConvert[pgsql.SyntaxNode](typedNode.Projection)...)
		nextCursor.AddBranches(pgsql.MustSliceTypeConvert[pgsql.SyntaxNode](typedNode.From)...)

		if typedNode.Where != nil {
			nextCursor.AddBranches(typedNode.Where)
		}

		nextCursor.AddBranches(pgsql.MustSliceTypeConvert[pgsql.SyntaxNode](typedNode.From)...)

		if typedNode.Having != nil {
			nextCursor.AddBranches(typedNode.Having)
		}

		return nextCursor, nil

	case pgsql.FromClause:
		nextCursor := &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Relation},
		}

		nextCursor.AddBranches(pgsql.MustSliceTypeConvert[pgsql.SyntaxNode](typedNode.Joins)...)
		return nextCursor, nil

	case pgsql.AliasedExpression:
		nextCursor := &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Expression},
		}

		if typedNode.Alias.Set {
			nextCursor.AddBranches(typedNode.Alias.Value)
		}

		return nextCursor, nil

	case pgsql.CompositeValue:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: pgsql.MustSliceTypeConvert[pgsql.SyntaxNode](typedNode.Values),
		}, nil

	case pgsql.TableReference:
		nextCursor := &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Name},
		}

		if typedNode.Binding.Set {
			nextCursor.AddBranches(typedNode.Binding.Value)
		}

		return nextCursor, nil

	case pgsql.TableAlias:
		nextCursor := &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Name},
		}

		if typedNode.Shape.Set {
			nextCursor.AddBranches(typedNode.Shape.Value)
		}

		return nextCursor, nil

	case pgsql.RowShape:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: pgsql.MustSliceTypeConvert[pgsql.SyntaxNode](typedNode.Columns),
		}, nil

	case pgsql.TypeCast:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Expression},
		}, nil

	case pgsql.Parenthetical:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Expression},
		}, nil

	case pgsql.FunctionCall:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: pgsql.MustSliceTypeConvert[pgsql.SyntaxNode](typedNode.Parameters),
		}, nil

	case *pgsql.FunctionCall:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: pgsql.MustSliceTypeConvert[pgsql.SyntaxNode](typedNode.Parameters),
		}, nil

	case pgsql.CompoundIdentifier, pgsql.Operator, pgsql.Literal, pgsql.Identifier, pgsql.Parameter, *pgsql.Parameter:
		return &Cursor[pgsql.SyntaxNode]{
			Node: node,
		}, nil

	case pgsql.ArrayLiteral:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: pgsql.MustSliceTypeConvert[pgsql.SyntaxNode](typedNode.Values),
		}, nil

	case pgsql.ArrayExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Expression},
		}, nil

	case pgsql.AnyExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Expression},
		}, nil

	case pgsql.UnaryExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Operand},
		}, nil

	case *pgsql.UnaryExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.Operand},
		}, nil

	case pgsql.BinaryExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.LOperand, typedNode.ROperand},
		}, nil

	case *pgsql.BinaryExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node:     node,
			Branches: []pgsql.SyntaxNode{typedNode.LOperand, typedNode.ROperand},
		}, nil

	case pgsql.CompoundExpression:
		return &Cursor[pgsql.SyntaxNode]{
			Node: node,
			Branches: pgsql.MustSliceTypeConvert[pgsql.SyntaxNode](typedNode.AsSlice()),
		}, nil

	case pgsql.ArrayIndex:
		return &Cursor[pgsql.SyntaxNode]{
			Node: node,
			Branches: append([]pgsql.SyntaxNode{typedNode.Expression}, pgsql.MustSliceTypeConvert[pgsql.SyntaxNode](typedNode.Indexes)...),
		}, nil

	default:
		return nil, fmt.Errorf("unable to negotiate sql type %T into a translation cursor", node)
	}
}
