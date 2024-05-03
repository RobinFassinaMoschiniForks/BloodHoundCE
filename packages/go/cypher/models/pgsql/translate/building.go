package translate

import (
	"fmt"
	"github.com/specterops/bloodhound/cypher/models"
	"github.com/specterops/bloodhound/cypher/models/pgsql"
)

func (s *Translator) buildProjection(scope *Scope) error {
	var (
		topLevelSelect = pgsql.Select{}
	)

	if projectionConstraint, err := s.treeTranslator.ConsumeAll(); err != nil {
		return err
	} else {
		topLevelSelect.Where = projectionConstraint.Expression
	}

	topLevelSelect.From = []pgsql.FromClause{{
		Relation: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{scope.Binding().Identifier},
		},
	}}

	for _, projection := range s.projections.Projections {
		if expressionReferences, err := ExtractSyntaxNodeReferences(projection.Expression); err != nil {
			return err
		} else {
			if err := RewriteExpressionCompoundIdentifiers(projection.Expression, expressionReferences, func(identifier pgsql.CompoundIdentifier) (pgsql.Expression, error) {
				return CompositeTypeFieldLookup(scope.Binding(), identifier), nil
			}); err != nil {
				return err
			}
		}
	}

	for _, projection := range s.projections.Projections {
		switch typedExpression := projection.Expression.(type) {
		case pgsql.Identifier:
			if binding, bound := scope.Lookup(typedExpression); !bound {
				s.SetErrorf("unknown identifier: %s", projection.Expression)
			} else if identifierProjection, err := binding.BuildProjection(scope); err != nil {
				s.SetError(err)
			} else {
				topLevelSelect.Projection = append(topLevelSelect.Projection, identifierProjection)
			}

		case pgsql.CompoundIdentifier:
			if binding, bound := scope.Lookup(typedExpression.Root()); !bound {
				s.SetErrorf("unknown identifier: %s", projection.Expression)
			} else if identifierProjection, err := binding.BuildProjection(scope); err != nil {
				s.SetError(err)
			} else {
				topLevelSelect.Projection = append(topLevelSelect.Projection, identifierProjection)
			}

		case *pgsql.BinaryExpression:
			topLevelSelect.Projection = append(topLevelSelect.Projection, pgsql.AliasedExpression{
				Expression: typedExpression,
				Alias:      projection.Alias,
			})

		default:
			return fmt.Errorf("unable to project type: %T", projection.Expression)
		}
	}

	s.translatedQuery.Body = topLevelSelect

	if s.query.Skip.Set {
		s.translatedQuery.Offset = s.query.Skip
	}

	if s.query.Limit.Set {
		s.translatedQuery.Limit = s.query.Limit
	}

	if len(s.query.OrderBy) > 0 {
		s.translatedQuery.OrderBy = s.query.OrderBy
	}

	return nil
}

func (s *Translator) buildPatternPart(scope *Scope, part *PatternPart) error {
	if part.IsTraversal {
		return s.buildPattern(scope, part)
	} else {
		return s.buildNodePattern(scope, part.NodeSelect.Identifier.Value)
	}
}

func (s *Translator) buildPattern(scope *Scope, pattern *PatternPart) error {
	for idx, traversalStep := range pattern.TraversalSteps {
		if traversalStep.Expansion.Set {
			if idx > 0 {
				if err := s.buildExpansionPatternStep(scope, traversalStep); err != nil {
					return err
				}
			} else {
				if err := s.buildExpansionPatternRoot(scope, traversalStep); err != nil {
					return err
				}
			}
		} else if idx > 0 {
			if err := s.buildTraversalPatternStep(scope, traversalStep); err != nil {
				return err
			}
		} else {
			if err := s.buildTraversalPatternRoot(scope, traversalStep); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Translator) buildTraversalPatternStep(scope *Scope, traversalStep *TraversalStep) error {
	// The left node of this step is already declared as the right node of the previous step
	scope.Declare(traversalStep.EdgeIdentifier.Value)
	scope.Declare(traversalStep.RightNodeIdentifier.Value)

	if rightNodeConstraints, err := s.treeTranslator.Consume(traversalStep.RightNodeIdentifier.Value); err != nil {
		return err
	} else if edgeConstraints, err := s.treeTranslator.ConsumeSet(scope.Visible()); err != nil {
		return err
	} else {
		var (
			nextSelect = pgsql.Select{
				Where: edgeConstraints.Expression,
			}
		)

		if rightNodeJoinConstraint, err := rightNodeTraversalStepConstraint(traversalStep); err != nil {
			return err
		} else if rightNodeJoinCondition, err := ConjoinExpressions([]pgsql.Expression{rightNodeConstraints.Expression, rightNodeJoinConstraint}); err != nil {
			return err
		} else {
			nextSelect.From = []pgsql.FromClause{{
				Relation: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{scope.Binding().Identifier},
				},
			}, {
				Relation: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
					Binding: models.ValueOptional(traversalStep.EdgeIdentifier.Value),
				},
				Joins: []pgsql.Join{{
					Table: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
						Binding: models.ValueOptional(traversalStep.RightNodeIdentifier.Value),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeInner,
						Constraint: rightNodeJoinCondition,
					},
				}},
			}}
		}

		// Sets binding scope
		if projections, err := s.buildBindingProjections(scope, traversalStep.EdgeIdentifier.Value, []Constraint{edgeConstraints, rightNodeConstraints}); err != nil {
			return err
		} else {
			nextSelect.Projection = projections
		}

		// Prepare the next select statement
		s.translatedQuery.AddCTE(pgsql.CommonTableExpression{
			Alias: pgsql.TableAlias{
				Name: traversalStep.EdgeIdentifier.Value,
			},
			Query: pgsql.Query{
				Body: nextSelect,
			},
		})
	}

	return nil
}

func (s *Translator) buildTraversalPatternRoot(scope *Scope, traversalStep *TraversalStep) error {
	scope.Declare(traversalStep.LeftNodeIdentifier.Value)
	scope.Declare(traversalStep.EdgeIdentifier.Value)
	scope.Declare(traversalStep.RightNodeIdentifier.Value)

	if leftNodeConstraints, err := s.treeTranslator.Consume(traversalStep.LeftNodeIdentifier.Value); err != nil {
		return err
	} else if rightNodeConstraints, err := s.treeTranslator.Consume(traversalStep.RightNodeIdentifier.Value); err != nil {
		return err
	} else if edgeConstraints, err := s.treeTranslator.ConsumeSet(scope.Visible()); err != nil {
		return err
	} else {
		var (
			nextSelect = pgsql.Select{
				Where: edgeConstraints.Expression,
			}
		)

		if scopeBinding := scope.Binding(); scopeBinding != nil {
			nextSelect.From = append(nextSelect.From, pgsql.FromClause{
				Relation: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{scope.Binding().Identifier},
				},
			})
		}

		if projections, err := s.buildBindingProjections(scope, traversalStep.EdgeIdentifier.Value, []Constraint{edgeConstraints, rightNodeConstraints}); err != nil {
			return err
		} else {
			nextSelect.Projection = projections
		}

		if leftNodeJoinConstraint, err := leftNodeTraversalStepConstraint(traversalStep); err != nil {
			return err
		} else if leftNodeJoinCondition, err := ConjoinExpressions([]pgsql.Expression{leftNodeConstraints.Expression, leftNodeJoinConstraint}); err != nil {
			return err
		} else if rightNodeJoinConstraint, err := rightNodeTraversalStepConstraint(traversalStep); err != nil {
			return err
		} else if rightNodeJoinCondition, err := ConjoinExpressions([]pgsql.Expression{rightNodeConstraints.Expression, rightNodeJoinConstraint}); err != nil {
			return err
		} else {
			nextSelect.From = append(nextSelect.From, pgsql.FromClause{
				Relation: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
					Binding: models.ValueOptional(traversalStep.EdgeIdentifier.Value),
				},
				Joins: []pgsql.Join{{
					Table: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
						Binding: models.ValueOptional(traversalStep.LeftNodeIdentifier.Value),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeInner,
						Constraint: leftNodeJoinCondition,
					},
				}, {
					Table: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
						Binding: models.ValueOptional(traversalStep.RightNodeIdentifier.Value),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeInner,
						Constraint: rightNodeJoinCondition,
					},
				}},
			})
		}

		// Prepare the next select statement
		s.translatedQuery.AddCTE(pgsql.CommonTableExpression{
			Alias: pgsql.TableAlias{
				Name: traversalStep.EdgeIdentifier.Value,
			},
			Query: pgsql.Query{
				Body: nextSelect,
			},
		})
	}

	return nil
}

func (s *Translator) buildBindingProjections(scope *Scope, scopeIdentifier pgsql.Identifier, constraints []Constraint) ([]pgsql.Projection, error) {
	var projections []pgsql.Projection

	if visibleBindings, err := scope.LookupBindings(scope.Visible().Slice()...); err != nil {
		return nil, err
	} else {
		for _, visibleBinding := range visibleBindings {
			if newProjections, err := s.bindVisibleProjection(scope, scopeIdentifier, visibleBinding, constraints); err != nil {
				return nil, err
			} else {
				projections = append(projections, newProjections...)
			}
		}
	}

	// Update scope binding
	if scopeBinding, bound := scope.Lookup(scopeIdentifier); !bound {
		return nil, fmt.Errorf("invalid identifier %s", scopeIdentifier)
	} else {
		scope.SetScopeBinding(scopeBinding)
	}

	return projections, nil
}

func (s *Translator) bindVisibleProjection(scope *Scope, nextScopeIdentifier pgsql.Identifier, visibleBinding *BoundIdentifier, constraints []Constraint) ([]pgsql.Projection, error) {
	if projection, err := s.buildVisibleProjection(visibleBinding, nextScopeIdentifier, constraints, scope.Binding()); err != nil {
		return nil, err
	} else if scopeBinding, bound := scope.Lookup(nextScopeIdentifier); !bound {
		return nil, fmt.Errorf("invalid identifier %s", nextScopeIdentifier)
	} else {
		// Update the scope binding of this identifier
		visibleBinding.ScopeBinding = scopeBinding
		return projection, nil
	}
}

func (s *Translator) buildVisibleProjection(visibleBinding *BoundIdentifier, scopeIdentifier pgsql.Identifier, constraints []Constraint, currentScopeBinding *BoundIdentifier) ([]pgsql.Projection, error) {
	switch visibleBinding.DataType {
	case pgsql.ExpansionPattern:
		// No projection for patterns
		return nil, nil

	case pgsql.ExpansionRootNode, pgsql.ExpansionTerminalNode:
		value := pgsql.CompositeValue{
			DataType: pgsql.NodeComposite,
		}

		for _, nodeTableColumn := range pgsql.NodeTableColumns {
			value.Values = append(value.Values, pgsql.CompoundIdentifier{visibleBinding.Identifier, nodeTableColumn})
		}

		// Change the type to the node composite now that this is projected
		visibleBinding.DataType = pgsql.NodeComposite

		// Create a new final projection that's aliased to the visible binding's identifier
		return []pgsql.Projection{
			pgsql.AliasedExpression{
				Expression: value,
				Alias:      pgsql.AsOptionalIdentifier(visibleBinding.Identifier),
			},
		}, nil

	case pgsql.NodeComposite:
		if visibleBinding.ScopeBinding == nil || visibleBinding.ScopeBinding.Identifier == scopeIdentifier {
			value := pgsql.CompositeValue{
				DataType: pgsql.NodeComposite,
			}

			for _, nodeTableColumn := range pgsql.NodeTableColumns {
				value.Values = append(value.Values, pgsql.CompoundIdentifier{visibleBinding.Identifier, nodeTableColumn})
			}

			// Create a new final projection that's aliased to the visible binding's identifier
			return []pgsql.Projection{
				pgsql.AliasedExpression{
					Expression: value,
					Alias:      pgsql.AsOptionalIdentifier(visibleBinding.Identifier),
				},
			}, nil
		} else {
			for _, constraint := range constraints {
				if err := RewriteExpressionCompoundIdentifier(constraint.Expression, visibleBinding.Identifier, func(identifier pgsql.CompoundIdentifier) (pgsql.Expression, error) {
					return CompositeTypeFieldLookup(currentScopeBinding, identifier), nil
				}); err != nil {
					return nil, err
				}
			}

			return []pgsql.Projection{
				pgsql.AliasedExpression{
					Expression: pgsql.CompoundIdentifier{visibleBinding.ScopeBinding.Identifier, visibleBinding.Identifier},
					Alias:      pgsql.AsOptionalIdentifier(visibleBinding.Identifier),
				},
			}, nil
		}

	case pgsql.ExpansionEdge:
		value := pgsql.CompositeValue{
			DataType: pgsql.EdgeComposite,
		}

		for _, edgeTableColumn := range pgsql.EdgeTableColumns {
			value.Values = append(value.Values, pgsql.CompoundIdentifier{visibleBinding.Identifier, edgeTableColumn})
		}

		// Change the type to the node composite now that this is projected
		visibleBinding.DataType = pgsql.EdgeComposite

		// Create a new final projection that's aliased to the visible binding's identifier
		return []pgsql.Projection{
			pgsql.AliasedExpression{
				Expression: pgsql.Parenthetical{
					Expression: pgsql.Select{
						Projection: []pgsql.Projection{
							pgsql.FunctionCall{
								Function:   pgsql.FunctionArrayAggregate,
								Parameters: []pgsql.Expression{value},
							},
						},
						From: []pgsql.FromClause{{
							Relation: pgsql.TableReference{
								Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
								Binding: models.ValueOptional(visibleBinding.Identifier),
							},
							Joins: nil,
						}},
						Where: pgsql.NewBinaryExpression(
							pgsql.CompoundIdentifier{visibleBinding.Identifier, pgsql.ColumnID},
							pgsql.OperatorEquals,
							pgsql.NewAnyExpression(
								pgsql.CompoundIdentifier{scopeIdentifier, expansionPath},
							),
						),
					},
				},
				Alias: pgsql.AsOptionalIdentifier(visibleBinding.Identifier),
			},
		}, nil

	case pgsql.EdgeComposite:
		if visibleBinding.ScopeBinding == nil || visibleBinding.ScopeBinding.Identifier == scopeIdentifier {
			value := pgsql.CompositeValue{
				DataType: pgsql.EdgeComposite,
			}

			for _, edgeTableColumn := range pgsql.EdgeTableColumns {
				value.Values = append(value.Values, pgsql.CompoundIdentifier{visibleBinding.Identifier, edgeTableColumn})
			}

			// Create a new final projection that's aliased to the visible binding's identifier
			return []pgsql.Projection{
				pgsql.AliasedExpression{
					Expression: value,
					Alias:      pgsql.AsOptionalIdentifier(visibleBinding.Identifier),
				},
			}, nil
		} else {
			for _, constraint := range constraints {
				if err := RewriteExpressionCompoundIdentifier(constraint.Expression, visibleBinding.Identifier, func(identifier pgsql.CompoundIdentifier) (pgsql.Expression, error) {
					return CompositeTypeFieldLookup(currentScopeBinding, identifier), nil
				}); err != nil {
					return nil, err
				}
			}

			return []pgsql.Projection{
				pgsql.AliasedExpression{
					Expression: pgsql.CompoundIdentifier{visibleBinding.ScopeBinding.Identifier, visibleBinding.Identifier},
					Alias:      pgsql.AsOptionalIdentifier(visibleBinding.Identifier),
				},
			}, nil
		}

	default:
		return nil, fmt.Errorf("unsupported projection type: %s", visibleBinding.DataType.String())
	}
}

func expansionConstraints(expansionIdentifier pgsql.Identifier) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(
		pgsql.UnaryExpression{
			Operator: pgsql.OperatorNot,
			Operand:  pgsql.CompoundIdentifier{expansionIdentifier, expansionIsCycle},
		},
		pgsql.OperatorAnd,
		pgsql.UnaryExpression{
			Operator: pgsql.OperatorNot,
			Operand:  pgsql.CompoundIdentifier{expansionIdentifier, expansionSatisfied},
		})
}

type ExpansionBuilder struct {
	PrimerStatement     pgsql.Select
	RecursiveStatement  pgsql.Select
	ProjectionStatement pgsql.Select
	Query               pgsql.Query
}

func (s ExpansionBuilder) Build(expansionIdentifier pgsql.Identifier) pgsql.Query {
	s.Query.AddCTE(pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name:  expansionIdentifier,
			Shape: models.ValueOptional(expansionColumns()),
		},
		Query: pgsql.Query{
			Body: pgsql.SetOperation{
				LOperand: s.PrimerStatement,
				ROperand: s.RecursiveStatement,
				Operator: pgsql.OperatorUnion,
			},
		},
	})

	s.Query.Body = s.ProjectionStatement
	return s.Query
}

func (s *Translator) buildExpansionPatternRoot(scope *Scope, traversalStep *TraversalStep) error {
	var (
		expansion = ExpansionBuilder{
			Query: pgsql.Query{
				CommonTableExpressions: &pgsql.With{
					Recursive: true,
				},
			},

			PrimerStatement: pgsql.Select{
				Projection: []pgsql.Projection{
					pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnStartID},
					pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnEndID},
					pgsql.MustAsLiteral(1),
					pgsql.MustAsLiteral(false),
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnStartID},
						pgsql.OperatorEquals,
						pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnEndID},
					),
					pgsql.ArrayLiteral{
						Values: []pgsql.Expression{
							pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
						},
					},
				},
			},

			RecursiveStatement: pgsql.Select{
				Projection: []pgsql.Projection{
					pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionRootID},
					pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnEndID},
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionDepth},
						pgsql.OperatorAdd,
						pgsql.MustAsLiteral(1),
					),
					pgsql.MustAsLiteral(false),
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
						pgsql.OperatorEquals,
						pgsql.NewAnyExpression(pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath}),
					),
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath},
						pgsql.OperatorConcatenate,
						pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
					),
				},

				Where: expansionConstraints(traversalStep.Expansion.Value.Identifier),
			},
		}
	)

	scope.Declare(traversalStep.Expansion.Value.Identifier)
	scope.Declare(traversalStep.LeftNodeIdentifier.Value)
	scope.Declare(traversalStep.EdgeIdentifier.Value)
	scope.Declare(traversalStep.RightNodeIdentifier.Value)

	if leftNodeConstraints, err := s.treeTranslator.Consume(traversalStep.LeftNodeIdentifier.Value); err != nil {
		return err
	} else if rightNodeConstraints, err := s.treeTranslator.Consume(traversalStep.RightNodeIdentifier.Value); err != nil {
		return err
	} else if edgeConstraints, err := s.treeTranslator.ConsumeSet(scope.Visible()); err != nil {
		return err
	} else {
		var (
			dependencies = edgeConstraints.Dependencies.Copy().Remove(traversalStep.EdgeIdentifier.Value)
		)

		if scope.Binding() != nil {
			// Ensure the scope binding is a dependency of building the next projection
			dependencies.Add(scope.Binding().Identifier)
		}

		if projections, err := s.buildBindingProjections(scope, traversalStep.Expansion.Value.Identifier, []Constraint{edgeConstraints, rightNodeConstraints}); err != nil {
			return err
		} else {
			expansion.ProjectionStatement.Projection = projections
		}

		if primerFromClauses, err := scope.BuildFromClauses(dependencies.Slice()...); err != nil {
			return err
		} else {
			expansion.PrimerStatement.From = primerFromClauses
		}

		// Set the edge constraints in the primer and recursive select where clauses
		expansion.PrimerStatement.Where = edgeConstraints.Expression
		expansion.RecursiveStatement.Where = edgeConstraints.Expression

		if leftNodeJoinConstraint, err := leftNodeTraversalStepConstraint(traversalStep); err != nil {
			return err
		} else if leftNodeJoinCondition, err := ConjoinExpressions([]pgsql.Expression{leftNodeConstraints.Expression, leftNodeJoinConstraint}); err != nil {
			return err
		} else if rightNodeJoinConstraint, err := rightNodeTraversalStepConstraint(traversalStep); err != nil {
			return err
		} else if rightNodeJoinCondition, err := ConjoinExpressions([]pgsql.Expression{rightNodeConstraints.Expression, rightNodeJoinConstraint}); err != nil {
			return err
		} else {
			expansion.PrimerStatement.From = append(expansion.PrimerStatement.From, pgsql.FromClause{
				Relation: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
					Binding: models.ValueOptional(traversalStep.EdgeIdentifier.Value),
				},
				Joins: []pgsql.Join{{
					Table: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
						Binding: models.ValueOptional(traversalStep.LeftNodeIdentifier.Value),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeInner,
						Constraint: leftNodeJoinCondition,
					},
				}, {
					Table: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
						Binding: models.ValueOptional(traversalStep.RightNodeIdentifier.Value),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeInner,
						Constraint: rightNodeJoinCondition,
					},
				}},
			})

			// Make sure the recursive query has the expansion bound
			expansion.RecursiveStatement.From = append(expansion.RecursiveStatement.From, pgsql.FromClause{
				Relation: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier},
				},
				Joins: []pgsql.Join{{
					Table: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
						Binding: models.ValueOptional(traversalStep.EdgeIdentifier.Value),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType: pgsql.JoinTypeInner,
						Constraint: pgsql.NewBinaryExpression(
							// TODO: Directional
							pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnStartID},
							pgsql.OperatorEquals,
							pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, pgsql.ColumnNextID},
						),
					},
				}, {
					Table: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
						Binding: models.ValueOptional(traversalStep.RightNodeIdentifier.Value),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeInner,
						Constraint: rightNodeJoinCondition,
					},
				}},
			})

			if wrappedSelectJoinConstraint, err := ConjoinExpressions([]pgsql.Expression{
				pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.ArrayIndex{
						Expression: pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath},
						Indexes: []pgsql.Expression{
							pgsql.FunctionCall{
								Function: pgsql.FunctionArrayLength,
								Parameters: []pgsql.Expression{
									pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath},
									pgsql.NewLiteral(1, pgsql.Int8),
								},
								CastType: pgsql.Int4,
							},
						},
					},
				),
				rightNodeJoinConstraint}); err != nil {
				return err
			} else {
				// Select the expansion components for the projection statement
				expansion.ProjectionStatement.From = append(expansion.ProjectionStatement.From, pgsql.FromClause{
					Relation: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier},
						Binding: models.EmptyOptional[pgsql.Identifier](),
					},
					Joins: []pgsql.Join{{
						Table: pgsql.TableReference{
							Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
							Binding: models.ValueOptional(traversalStep.EdgeIdentifier.Value),
						},
						JoinOperator: pgsql.JoinOperator{
							JoinType: pgsql.JoinTypeInner,
							Constraint: pgsql.NewBinaryExpression(
								pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
								pgsql.OperatorEquals,
								pgsql.NewAnyExpression(pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath}),
							),
						},
					}, {
						Table: pgsql.TableReference{
							Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
							Binding: models.ValueOptional(traversalStep.LeftNodeIdentifier.Value),
						},
						JoinOperator: pgsql.JoinOperator{
							JoinType: pgsql.JoinTypeInner,
							Constraint: pgsql.NewBinaryExpression(
								pgsql.CompoundIdentifier{traversalStep.LeftNodeIdentifier.Value, pgsql.ColumnID},
								pgsql.OperatorEquals,
								pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionRootID},
							),
						},
					}, {
						Table: pgsql.TableReference{
							Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
							Binding: models.ValueOptional(traversalStep.RightNodeIdentifier.Value),
						},
						JoinOperator: pgsql.JoinOperator{
							JoinType:   pgsql.JoinTypeInner,
							Constraint: wrappedSelectJoinConstraint,
						},
					}},
				})
			}
		}

		// If there are terminal constraints, project them as part of the recursive lookup
		if rightNodeConstraints.Expression != nil {
			if terminalCriteriaProjection, err := pgsql.As[pgsql.Projection](rightNodeConstraints.Expression); err != nil {
				return err
			} else {
				expansion.RecursiveStatement.Projection = []pgsql.Projection{
					pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionRootID},
					pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnEndID},
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionDepth},
						pgsql.OperatorAdd,
						pgsql.MustAsLiteral(1),
					),
					terminalCriteriaProjection,
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
						pgsql.OperatorEquals,
						pgsql.NewAnyExpression(pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath}),
					),
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath},
						pgsql.OperatorConcatenate,
						pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
					),
				}
			}
		}
	}

	// Append the wrapper query as a CTE for the translated query
	s.translatedQuery.AddCTE(pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name: traversalStep.Expansion.Value.Identifier,
		},
		Query: expansion.Build(traversalStep.Expansion.Value.Identifier),
	})

	// Update scope binding
	if scopeBinding, bound := scope.Lookup(traversalStep.Expansion.Value.Identifier); !bound {
		return fmt.Errorf("invalid identifier %s", traversalStep.Expansion.Value.Identifier)
	} else {
		scope.SetScopeBinding(scopeBinding)
	}

	return nil
}

func (s *Translator) buildExpansionPatternStep(scope *Scope, traversalStep *TraversalStep) error {
	var (
		expansion = ExpansionBuilder{
			Query: pgsql.Query{
				CommonTableExpressions: &pgsql.With{
					Recursive: true,
				},
			},

			PrimerStatement: pgsql.Select{
				Projection: []pgsql.Projection{
					pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnStartID},
					pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnEndID},
					pgsql.MustAsLiteral(1),
					pgsql.MustAsLiteral(false),
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnStartID},
						pgsql.OperatorEquals,
						pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnEndID},
					),
					pgsql.ArrayLiteral{
						Values: []pgsql.Expression{
							pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
						},
					},
				},
			},

			RecursiveStatement: pgsql.Select{
				Projection: []pgsql.Projection{
					pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionRootID},
					pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnEndID},
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionDepth},
						pgsql.OperatorAdd,
						pgsql.MustAsLiteral(1),
					),
					pgsql.MustAsLiteral(false),
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
						pgsql.OperatorEquals,
						pgsql.NewAnyExpression(pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath}),
					),
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath},
						pgsql.OperatorConcatenate,
						pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
					),
				},

				Where: expansionConstraints(traversalStep.Expansion.Value.Identifier),
			},
		}
	)

	scope.Declare(traversalStep.Expansion.Value.Identifier)
	scope.Declare(traversalStep.LeftNodeIdentifier.Value)
	scope.Declare(traversalStep.EdgeIdentifier.Value)
	scope.Declare(traversalStep.RightNodeIdentifier.Value)

	if rightNodeConstraints, err := s.treeTranslator.Consume(traversalStep.RightNodeIdentifier.Value); err != nil {
		return err
	} else if edgeConstraints, err := s.treeTranslator.ConsumeSet(scope.Visible()); err != nil {
		return err
	} else {
		if rightNodeJoinConstraint, err := rightNodeTraversalStepConstraint(traversalStep); err != nil {
			return err
		} else if rightNodeJoinCondition, err := ConjoinExpressions([]pgsql.Expression{rightNodeConstraints.Expression, rightNodeJoinConstraint}); err != nil {
			return err
		} else {
			expansion.PrimerStatement.From = append(expansion.PrimerStatement.From, pgsql.FromClause{
				Relation: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{scope.Binding().Identifier},
				},
				Joins: []pgsql.Join{{
					Table: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
						Binding: models.ValueOptional(traversalStep.EdgeIdentifier.Value),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeInner,
						Constraint: edgeConstraints.Expression,
					},
				}, {
					Table: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
						Binding: models.ValueOptional(traversalStep.RightNodeIdentifier.Value),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeInner,
						Constraint: rightNodeJoinCondition,
					},
				}},
			})

			// Make sure the recursive query has the expansion bound
			expansion.RecursiveStatement.From = append(expansion.RecursiveStatement.From, pgsql.FromClause{
				Relation: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier},
				},
				Joins: []pgsql.Join{{
					Table: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
						Binding: models.ValueOptional(traversalStep.EdgeIdentifier.Value),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType: pgsql.JoinTypeInner,
						Constraint: pgsql.NewBinaryExpression(
							// TODO: Directional
							pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnStartID},
							pgsql.OperatorEquals,
							pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, pgsql.ColumnNextID},
						),
					},
				}, {
					Table: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
						Binding: models.ValueOptional(traversalStep.RightNodeIdentifier.Value),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeInner,
						Constraint: rightNodeJoinCondition,
					},
				}},
			})

			if wrappedSelectJoinConstraint, err := ConjoinExpressions([]pgsql.Expression{
				pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.ArrayIndex{
						Expression: pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath},
						Indexes: []pgsql.Expression{
							pgsql.FunctionCall{
								Function: pgsql.FunctionArrayLength,
								Parameters: []pgsql.Expression{
									pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath},
									pgsql.NewLiteral(1, pgsql.Int8),
								},
								CastType: pgsql.Int4,
							},
						},
					},
				),
				rightNodeJoinConstraint}); err != nil {
				return err
			} else {
				// Select the expansion components for the projection statement
				expansion.ProjectionStatement.From = append(expansion.ProjectionStatement.From, pgsql.FromClause{
					Relation: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{scope.Binding().Identifier},
						Binding: models.EmptyOptional[pgsql.Identifier](),
					},
				})

				expansion.ProjectionStatement.From = append(expansion.ProjectionStatement.From, pgsql.FromClause{
					Relation: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier},
						Binding: models.EmptyOptional[pgsql.Identifier](),
					},
					Joins: []pgsql.Join{{
						Table: pgsql.TableReference{
							Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
							Binding: models.ValueOptional(traversalStep.EdgeIdentifier.Value),
						},
						JoinOperator: pgsql.JoinOperator{
							JoinType: pgsql.JoinTypeInner,
							Constraint: pgsql.NewBinaryExpression(
								pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
								pgsql.OperatorEquals,
								pgsql.NewAnyExpression(pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath}),
							),
						},
					}, {
						Table: pgsql.TableReference{
							Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
							Binding: models.ValueOptional(traversalStep.LeftNodeIdentifier.Value),
						},
						JoinOperator: pgsql.JoinOperator{
							JoinType: pgsql.JoinTypeInner,
							Constraint: pgsql.NewBinaryExpression(
								pgsql.CompoundIdentifier{traversalStep.LeftNodeIdentifier.Value, pgsql.ColumnID},
								pgsql.OperatorEquals,
								pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionRootID},
							),
						},
					}, {
						Table: pgsql.TableReference{
							Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
							Binding: models.ValueOptional(traversalStep.RightNodeIdentifier.Value),
						},
						JoinOperator: pgsql.JoinOperator{
							JoinType:   pgsql.JoinTypeInner,
							Constraint: wrappedSelectJoinConstraint,
						},
					}},
				})
			}
		}

		// If there are terminal constraints, project them as part of the recursive lookup
		if rightNodeConstraints.Expression != nil {
			if terminalCriteriaProjection, err := pgsql.As[pgsql.Projection](rightNodeConstraints.Expression); err != nil {
				return err
			} else {
				expansion.RecursiveStatement.Projection = []pgsql.Projection{
					pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionRootID},
					pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnEndID},
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionDepth},
						pgsql.OperatorAdd,
						pgsql.MustAsLiteral(1),
					),
					terminalCriteriaProjection,
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
						pgsql.OperatorEquals,
						pgsql.NewAnyExpression(pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath}),
					),
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.Expansion.Value.Identifier, expansionPath},
						pgsql.OperatorConcatenate,
						pgsql.CompoundIdentifier{traversalStep.EdgeIdentifier.Value, pgsql.ColumnID},
					),
				}
			}
		}

		var (
			dependencies = edgeConstraints.Dependencies.Copy().Remove(traversalStep.EdgeIdentifier.Value)
		)

		if scope.Binding() != nil {
			// Ensure the scope binding is a dependency of building the next projection
			dependencies.Add(scope.Binding().Identifier)
		}

		if projections, err := s.buildBindingProjections(scope, traversalStep.Expansion.Value.Identifier, []Constraint{edgeConstraints, rightNodeConstraints}); err != nil {
			return err
		} else {
			expansion.ProjectionStatement.Projection = projections
		}
	}

	// Append the wrapper query as a CTE for the translated query
	s.translatedQuery.AddCTE(pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name: traversalStep.Expansion.Value.Identifier,
		},
		Query: expansion.Build(traversalStep.Expansion.Value.Identifier),
	})

	// Update scope binding
	if scopeBinding, bound := scope.Lookup(traversalStep.Expansion.Value.Identifier); !bound {
		return fmt.Errorf("invalid identifier %s", traversalStep.Expansion.Value.Identifier)
	} else {
		scope.SetScopeBinding(scopeBinding)
	}

	return nil
}

func (s *Translator) buildEdgePattern(scope *Scope, traversalStep *TraversalStep) error {
	// Declare the end node identifier
	scope.Declare(traversalStep.RightNodeIdentifier.Value)

	if constraints, err := s.treeTranslator.Consume(traversalStep.RightNodeIdentifier.Value); err != nil {
		return err
	} else {
		// If there are no constraints, ensure that the identifier of this pattern is a dependency
		constraints.Dependencies.Add(traversalStep.RightNodeIdentifier.Value)

		// Author the required identifiers as from clauses
		if fromClauses, err := scope.BuildFromClauses(constraints.Dependencies.Slice()...); err != nil {
			s.SetError(err)
		} else {
			// Prepare the next select statement
			s.translatedQuery.AddCTE(pgsql.CommonTableExpression{
				Alias: pgsql.TableAlias{
					Name: traversalStep.RightNodeIdentifier.Value,
				},
				Query: pgsql.Query{
					Body: pgsql.Select{
						Projection: []pgsql.Projection{pgsql.AsWildcardIdentifier(traversalStep.RightNodeIdentifier.Value)},
						From:       fromClauses,
						Where:      constraints.Expression,
					},
				},
			})
		}
	}

	// Declare the edge identifier
	scope.Declare(traversalStep.EdgeIdentifier.Value)

	if constraints, err := s.treeTranslator.ConsumeSet(scope.Visible()); err != nil {
		return err
	} else {
		// If there are no constraints, ensure that the identifier of this pattern is a dependency
		constraints.Dependencies.Add(traversalStep.RightNodeIdentifier.Value)

		// Author the required identifiers as from clauses
		if fromClauses, err := scope.BuildFromClauses(constraints.Dependencies.Slice()...); err != nil {
			s.SetError(err)
		} else {
			// Prepare the next select statement
			s.translatedQuery.AddCTE(pgsql.CommonTableExpression{
				Alias: pgsql.TableAlias{
					Name: traversalStep.EdgeIdentifier.Value,
				},
				Query: pgsql.Query{
					Body: pgsql.Select{
						Projection: []pgsql.Projection{pgsql.AsWildcardIdentifier(traversalStep.EdgeIdentifier.Value)},
						From:       fromClauses,
						Where:      constraints.Expression,
					},
				},
			})
		}
	}

	return nil
}

func (s *Translator) buildNodePattern(scope *Scope, nodeIdentifier pgsql.Identifier) error {
	// Declare the node identifier
	scope.Declare(nodeIdentifier)

	if constraints, err := s.treeTranslator.ConsumeSet(scope.Visible()); err != nil {
		return err
	} else {
		var (
			// Ensure that the identifier of this pattern is a dependency of building the next scope projection
			nextSelect = pgsql.Select{
				Where: constraints.Expression,
			}
		)

		if scopeBinding := scope.Binding(); scopeBinding != nil {
			nextSelect.From = append(nextSelect.From, pgsql.FromClause{
				Relation: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{scope.Binding().Identifier},
				},
			})
		}

		if projections, err := s.buildBindingProjections(scope, nodeIdentifier, []Constraint{constraints}); err != nil {
			return err
		} else {
			nextSelect.Projection = projections
		}

		nextSelect.From = append(nextSelect.From, pgsql.FromClause{
			Relation: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.ValueOptional(nodeIdentifier),
			},
		})

		// Prepare the next select statement
		s.translatedQuery.AddCTE(pgsql.CommonTableExpression{
			Alias: pgsql.TableAlias{
				Name: nodeIdentifier,
			},
			Query: pgsql.Query{
				Body: nextSelect,
			},
		})
	}

	return nil
}

func CompositeTypeFieldLookup(scopeBinding *BoundIdentifier, compositeFieldLookup pgsql.CompoundIdentifier) pgsql.CompoundExpression {
	return pgsql.CompoundExpression{
		pgsql.Parenthetical{
			Expression: append(pgsql.CompoundIdentifier{scopeBinding.Identifier}, compositeFieldLookup.Root()),
		},

		compositeFieldLookup[1:],
	}
}

func (s *Translator) buildUpdates(scope *Scope) error {
	for _, assignment := range s.update.Assignments {
		var sqlUpdate pgsql.Update

		sqlUpdate.From = append(sqlUpdate.From, pgsql.FromClause{
			Relation: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{scope.Binding().Identifier},
			},
		})

		if constraints, err := s.treeTranslator.ConsumeSet(scope.Visible()); err != nil {
			return err
		} else {
			// Build the update selection clause
			if updateSelection, err := ConjoinExpressions([]pgsql.Expression{
				constraints.Expression,
				pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{assignment.Target.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{assignment.Binding.Identifier, pgsql.ColumnID}),
			}); err != nil {
				return err
			} else if projections, err := s.buildBindingProjections(scope, assignment.Binding.Identifier, []Constraint{{
				Dependencies: constraints.Dependencies.Copy(),
				Expression:   updateSelection,
			}}); err != nil {
				return err
			} else {
				for _, projection := range projections {
					switch typedProjection := projection.(type) {
					case pgsql.AliasedExpression:
						if !typedProjection.Alias.Set {
							return fmt.Errorf("expected aliased expression to have an alias set")
						} else if typedProjection.Alias.Value == assignment.Target.Identifier {
							// Strip the binding's scope
							assignment.Target.ScopeBinding = nil

							// This is the projection being replaced by the assignment
							if rewrittenProjections, err := s.bindVisibleProjection(scope, assignment.Binding.Identifier, assignment.Target, nil); err != nil {
								return err
							} else {
								for _, rewrittenProjection := range rewrittenProjections {
									if err := RewriteExpressionCompoundIdentifier(rewrittenProjection, assignment.Target.Identifier, func(identifier pgsql.CompoundIdentifier) (pgsql.Expression, error) {
										return append(pgsql.CompoundIdentifier{assignment.Binding.Identifier}, identifier[1:]...), nil
									}); err != nil {
										return err
									} else {
										sqlUpdate.Returning = append(sqlUpdate.Returning, rewrittenProjection)
									}
								}
							}

							continue
						}

						sqlUpdate.Returning = append(sqlUpdate.Returning, typedProjection)

					default:
						return fmt.Errorf("expected aliased expression as projection but got: %T", projection)
					}
				}

				sqlUpdate.Where = models.ValueOptional(updateSelection)
			}
		}

		switch assignment.Binding.DataType {
		case pgsql.NodeComposite:
			sqlUpdate.Table = pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.ValueOptional(assignment.Binding.Identifier),
			}

		case pgsql.EdgeComposite:
			sqlUpdate.Table = pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
				Binding: models.ValueOptional(assignment.Binding.Identifier),
			}

		default:
			return fmt.Errorf("invalid identifier data type for update: %s", assignment.Binding.Identifier)
		}

		if err := RewriteExpressionCompoundIdentifier(assignment.Expression, assignment.Target.Identifier, func(identifier pgsql.CompoundIdentifier) (pgsql.Expression, error) {
			return append(pgsql.CompoundIdentifier{assignment.Binding.Identifier}, identifier[1:]...), nil
		}); err != nil {
			return err
		}

		sqlUpdate.Assignments = []pgsql.Assignment{assignment.Expression}

		s.translatedQuery.AddCTE(pgsql.CommonTableExpression{
			Alias: pgsql.TableAlias{
				Name: assignment.Binding.Identifier,
			},
			Query: pgsql.Query{
				Body: sqlUpdate,
			},
		})
	}

	return nil
}

func (s *Translator) buildMatch(scope *Scope) error {
	for _, part := range s.pattern.Parts {
		// Pattern can't be in scope at time of select as the pattern's scope directly depends on the
		// pattern parts
		if err := s.buildPatternPart(scope, part); err != nil {
			return err
		}

		// Declare the pattern variable in scope if set
		if part.PatternBinding.Set {
			scope.Declare(part.PatternBinding.Value.Identifier)
		}
	}

	return nil
}
