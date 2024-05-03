package translate

import (
	"encoding/json"
	"fmt"
	cypher "github.com/specterops/bloodhound/cypher/model"
	"github.com/specterops/bloodhound/cypher/models"
	"github.com/specterops/bloodhound/cypher/models/pgsql"
	"github.com/specterops/bloodhound/dawgs/graph"
	"strings"
)

func translateCypherAssignmentOperator(operator cypher.AssignmentOperator) (pgsql.Operator, error) {
	switch operator {
	case cypher.OperatorAssignment:
		return pgsql.OperatorAssignment, nil
	case cypher.OperatorAdditionAssignment:
		return pgsql.OperatorAdditionAssignment, nil
	case cypher.OperatorLabelAssignment:
		return pgsql.OperatorLabelAssignment, nil
	default:
		return pgsql.UnsetOperator, fmt.Errorf("unknown assignment operator %s", operator)
	}
}

func leftEdgeConstraint(rootEdge, terminalEdge pgsql.Identifier, direction graph.Direction) (pgsql.Expression, error) {
	switch direction {
	case graph.DirectionOutbound:
		return &pgsql.BinaryExpression{
			Operator: pgsql.OperatorEquals,
			ROperand: pgsql.CompoundIdentifier{rootEdge, pgsql.ColumnStartID},
			LOperand: pgsql.CompoundIdentifier{terminalEdge, pgsql.ColumnEndID},
		}, nil

	case graph.DirectionInbound:
		return &pgsql.BinaryExpression{
			Operator: pgsql.OperatorEquals,
			ROperand: pgsql.CompoundIdentifier{rootEdge, pgsql.ColumnEndID},
			LOperand: pgsql.CompoundIdentifier{terminalEdge, pgsql.ColumnStartID},
		}, nil

	default:
		return nil, fmt.Errorf("unsupported direction: %d", direction)
	}
}

func leftNodeConstraint(edgeIdentifier, nodeIdentifier pgsql.Identifier, direction graph.Direction) (pgsql.Expression, error) {
	switch direction {
	case graph.DirectionOutbound:
		return &pgsql.BinaryExpression{
			Operator: pgsql.OperatorEquals,
			ROperand: pgsql.CompoundIdentifier{edgeIdentifier, pgsql.ColumnStartID},
			LOperand: pgsql.CompoundIdentifier{nodeIdentifier, pgsql.ColumnID},
		}, nil

	case graph.DirectionInbound:
		return &pgsql.BinaryExpression{
			Operator: pgsql.OperatorEquals,
			ROperand: pgsql.CompoundIdentifier{edgeIdentifier, pgsql.ColumnEndID},
			LOperand: pgsql.CompoundIdentifier{nodeIdentifier, pgsql.ColumnID},
		}, nil

	default:
		return nil, fmt.Errorf("unsupported direction: %d", direction)
	}
}

func leftNodeTraversalStepConstraint(traversalStep *TraversalStep) (pgsql.Expression, error) {
	return leftNodeConstraint(
		traversalStep.EdgeIdentifier.Value,
		traversalStep.LeftNodeIdentifier.Value,
		traversalStep.Direction)
}

func rightEdgeConstraint(rootEdge *BoundIdentifier, terminalEdge pgsql.Identifier, direction graph.Direction) (pgsql.Expression, error) {
	switch rootEdge.DataType {
	case pgsql.EdgeComposite:
		switch direction {
		case graph.DirectionOutbound:
			return &pgsql.BinaryExpression{
				Operator: pgsql.OperatorEquals,
				LOperand: pgsql.CompoundIdentifier{rootEdge.Identifier, pgsql.ColumnEndID},
				ROperand: pgsql.CompoundIdentifier{terminalEdge, pgsql.ColumnStartID},
			}, nil

		case graph.DirectionInbound:
			return &pgsql.BinaryExpression{
				Operator: pgsql.OperatorEquals,
				LOperand: pgsql.CompoundIdentifier{rootEdge.Identifier, pgsql.ColumnStartID},
				ROperand: pgsql.CompoundIdentifier{terminalEdge, pgsql.ColumnEndID},
			}, nil

		default:
			return nil, fmt.Errorf("unsupported direction: %d", direction)
		}

	case pgsql.ExpansionEdge:
		switch direction {
		case graph.DirectionOutbound:
			return pgsql.NewBinaryExpression(
				pgsql.CompoundExpression{
					pgsql.ArrayIndex{
						Expression: rootEdge.Identifier,
						Indexes: []pgsql.Expression{
							pgsql.FunctionCall{
								Function: pgsql.FunctionArrayLength,
								Parameters: []pgsql.Expression{
									rootEdge.Identifier,
									pgsql.NewLiteral(1, pgsql.Int),
								},
								CastType: pgsql.Int,
							},
						},
					},
					pgsql.ColumnEndID,
				},
				pgsql.OperatorEquals,
				pgsql.CompoundIdentifier{terminalEdge, pgsql.ColumnStartID},
			), nil

		case graph.DirectionInbound:
			return &pgsql.BinaryExpression{
				Operator: pgsql.OperatorEquals,
				LOperand: pgsql.CompoundIdentifier{rootEdge.Identifier, pgsql.ColumnStartID},
				ROperand: pgsql.CompoundIdentifier{terminalEdge, pgsql.ColumnEndID},
			}, nil

		default:
			return nil, fmt.Errorf("unsupported direction: %d", direction)
		}

	default:
		return nil, fmt.Errorf("invalid root edge type: %s", rootEdge.DataType)
	}
}

func rightNodeConstraint(edgeIdentifier, nodeIdentifier pgsql.Identifier, direction graph.Direction) (pgsql.Expression, error) {
	switch direction {
	case graph.DirectionOutbound:
		return &pgsql.BinaryExpression{
			Operator: pgsql.OperatorEquals,
			ROperand: pgsql.CompoundIdentifier{edgeIdentifier, pgsql.ColumnEndID},
			LOperand: pgsql.CompoundIdentifier{nodeIdentifier, pgsql.ColumnID},
		}, nil

	case graph.DirectionInbound:
		return &pgsql.BinaryExpression{
			Operator: pgsql.OperatorEquals,
			ROperand: pgsql.CompoundIdentifier{edgeIdentifier, pgsql.ColumnStartID},
			LOperand: pgsql.CompoundIdentifier{nodeIdentifier, pgsql.ColumnID},
		}, nil

	default:
		return nil, fmt.Errorf("unsupported direction: %d", direction)
	}
}

func rightNodeTraversalStepConstraint(traversalStep *TraversalStep) (pgsql.Expression, error) {
	return rightNodeConstraint(
		traversalStep.EdgeIdentifier.Value,
		traversalStep.RightNodeIdentifier.Value,
		traversalStep.Direction)
}

func (s *Translator) translateSetOperation(scope *Scope, operator pgsql.Operator) error {
	if rightOperand, err := s.treeTranslator.Pop(); err != nil {
		return err
	} else if leftOperand, err := s.treeTranslator.Pop(); err != nil {
		return err
	} else if leftPropertyLookup, err := decomposePropertyLookup(leftOperand); err != nil {
		return err
	} else {
		if _, isPropertyLookup := asPropertyLookup(rightOperand); isPropertyLookup {
			return fmt.Errorf("unsupported")
		}

		// Literal or parameter for right hand side
		switch typedRightOperand := rightOperand.(type) {
		case *pgsql.BinaryExpression:
			return fmt.Errorf("unsupported")

		case pgsql.Literal:
			// Recast this literal as a JSONB value if it isn't a string
			if typedRightOperand.CastType != pgsql.Text {
				if jsonBytes, err := json.Marshal(typedRightOperand.Value); err != nil {
					return fmt.Errorf("unable to marshal literal as JSON: %w", err)
				} else {
					rightOperand = pgsql.NewLiteral(string(jsonBytes), pgsql.JSONB)
				}
			}
		}

		if assignment, err := s.update.NewAssignment(scope, leftPropertyLookup.Reference.Root()); err != nil {
			return err
		} else if jsonPath, err := pgsql.NewArrayLiteral([]pgsql.Expression{leftPropertyLookup.Field}, pgsql.Text); err != nil {
			return err
		} else {
			assignment.Expression = pgsql.NewBinaryExpression(
				pgsql.ColumnProperties,
				operator,
				pgsql.FunctionCall{
					Function: pgsql.FunctionJSONBSet,
					Parameters: []pgsql.Expression{
						pgsql.CompoundIdentifier{assignment.Target.Identifier, pgsql.ColumnProperties},
						jsonPath,
						rightOperand,
					},
					CastType: pgsql.JSONB,
				},
			)
		}
	}

	return nil
}

func (s *Translator) translateDateTimeFunctionCall(cypherFunc *cypher.FunctionInvocation, dataType pgsql.DataType) error {
	// Ensure the local date time function uses the default precision
	const defaultTimestampPrecision = 6

	var functionIdentifier pgsql.Identifier

	switch dataType {
	case pgsql.Date:
		functionIdentifier = pgsql.FunctionCurrentDate

	case pgsql.TimeWithoutTimeZone:
		functionIdentifier = pgsql.FunctionLocalTime

	case pgsql.TimeWithTimeZone:
		functionIdentifier = pgsql.FunctionCurrentTime

	case pgsql.TimestampWithoutTimeZone:
		functionIdentifier = pgsql.FunctionLocalTimestamp

	case pgsql.TimestampWithTimeZone:
		functionIdentifier = pgsql.FunctionNow

	default:
		return fmt.Errorf("unable to convert date function with data type: %s", dataType)
	}

	// Apply defaults for this function
	if !cypherFunc.HasArguments() {
		switch functionIdentifier {
		case pgsql.FunctionCurrentDate:
			s.treeTranslator.Push(pgsql.FunctionCall{
				Function: functionIdentifier,
				Bare:     true,
				CastType: dataType,
			})

		case pgsql.FunctionNow:
			s.treeTranslator.Push(pgsql.FunctionCall{
				Function: functionIdentifier,
				Bare:     false,
				CastType: dataType,
			})

		default:
			if precisionLiteral, err := pgsql.AsLiteral(defaultTimestampPrecision); err != nil {
				return err
			} else {
				s.treeTranslator.Push(pgsql.FunctionCall{
					Function: functionIdentifier,
					Parameters: []pgsql.Expression{
						precisionLiteral,
					},
					CastType: dataType,
				})
			}
		}
	} else if cypherFunc.NumArguments() > 1 {
		return fmt.Errorf("expected only one text argument for cypher function: %s", cypherFunc.Name)
	} else if specArgument, err := s.treeTranslator.Pop(); err != nil {
		return err
	} else {
		s.treeTranslator.Push(pgsql.NewTypeCast(specArgument, dataType))
	}

	return nil
}

func (s *Translator) translateNodePatternToStep(scope *Scope, nodeBinding *BoundIdentifier) error {
	var (
		part                   = s.pattern.CurrentPart()
		nodeIdentifierOptional = models.ValueOptional(nodeBinding.Identifier)
	)

	if part.PatternBinding.Set {
		part.PatternBinding.Value.DependOn(nodeBinding)
	}

	if part.IsTraversal {
		if numSteps := len(part.TraversalSteps); numSteps == 0 {
			// This is the traversal step's left node
			part.TraversalSteps = append(part.TraversalSteps, &TraversalStep{
				LeftNodeIdentifier: nodeIdentifierOptional,
			})
		} else if currentStep := part.TraversalSteps[numSteps-1]; !currentStep.RightNodeIdentifier.Set {
			// Set the right node pattern identifier
			currentStep.RightNodeIdentifier = nodeIdentifierOptional

			// This is part of a continuing pattern element chain. Inspect the previous edge pattern to see if this
			// is the terminal node of an expansion.
			if edgeBinding, bound := scope.Lookup(currentStep.EdgeIdentifier.Value); !bound {
				return fmt.Errorf("invalid identifier: %s", currentStep.RightNodeIdentifier.Value)
			} else if edgeBinding.DataType == pgsql.ExpansionEdge {
				nodeBinding.DataType = pgsql.ExpansionTerminalNode

				// If the edge is an expansion link the node as the right terminal node to the expansion
				if expansionBinding, found := edgeBinding.FirstDependencyByType(pgsql.ExpansionPattern); !found {
					return fmt.Errorf("unable to find expansion context for node: %s", nodeBinding.Identifier)
				} else {
					nodeBinding.Link(expansionBinding)
				}
			} else {
				// Scope the right node to the edge
				nodeBinding.ScopeBinding = edgeBinding
			}
		} else {
			return fmt.Errorf("unpacked too many nodes for node pattern")
		}
	} else {
		// If this isn't a traversal of any kind, store the identifier reference
		part.NodeSelect.Identifier = nodeIdentifierOptional
	}

	return nil
}

func (s *Translator) translateKindMatcher(kindMatcher *cypher.KindMatcher) error {
	if variable, isVariable := kindMatcher.Reference.(*cypher.Variable); !isVariable {
		return fmt.Errorf("expected variable for kind matcher reference but found type: %T", kindMatcher.Reference)
	} else if binding, resolved := s.query.Scope.LookupString(variable.Symbol); !resolved {
		return fmt.Errorf("unable to find identifier %s", variable.Symbol)
	} else if kindIDs, missingKinds := s.kindMapper.MapKinds(kindMatcher.Kinds); len(missingKinds) > 0 {
		return fmt.Errorf("unable to map kinds: %s", strings.Join(missingKinds.Strings(), ", "))
	} else if kindIDsLiteral, err := pgsql.AsLiteral(kindIDs); err != nil {
		return err
	} else {
		switch binding.DataType {
		case pgsql.NodeComposite:
			s.treeTranslator.Push(pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnKindIDs})
			s.treeTranslator.Push(kindIDsLiteral)

			if err := s.treeTranslator.PopPushOperator(pgsql.OperatorPGArrayOverlap); err != nil {
				s.SetError(err)
			}

		case pgsql.EdgeComposite:
			s.treeTranslator.Push(pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnKindID})
			s.treeTranslator.Push(pgsql.NewAnyExpression(kindIDsLiteral))

			if err := s.treeTranslator.PopPushOperator(pgsql.OperatorEquals); err != nil {
				s.SetError(err)
			}

		default:
			return fmt.Errorf("unexpected kind matcher reference data type: %s", binding.DataType)
		}
	}

	return nil
}

func (s *Translator) translateProjection(scope *Scope, projection *cypher.Projection) error {
	s.projections = NewProjectionClause()
	s.projections.Distinct = projection.Distinct

	if projection.Skip != nil {
		if cypherLiteral, isLiteral := projection.Skip.Value.(*cypher.Literal); !isLiteral {
			return fmt.Errorf("expected a literal skip value but received: %T", projection.Skip.Value)
		} else if pgLiteral, err := pgsql.AsLiteral(cypherLiteral.Value); err != nil {
			return err
		} else {
			s.query.Skip = models.ValueOptional[pgsql.Expression](pgLiteral)
		}
	}

	if projection.Limit != nil {
		if cypherLiteral, isLiteral := projection.Limit.Value.(*cypher.Literal); !isLiteral {
			return fmt.Errorf("expected a literal limit value but received: %T", projection.Limit.Value)
		} else if pgLiteral, err := pgsql.AsLiteral(cypherLiteral.Value); err != nil {
			return err
		} else {
			s.query.Limit = models.ValueOptional[pgsql.Expression](pgLiteral)
		}
	}

	return nil
}

func (s *Translator) bindNodePattern(scope *Scope, pattern *cypher.NodePattern) (*BoundIdentifier, error) {
	if binding, err := scope.DefineNew(pgsql.NodeComposite); err != nil {
		return nil, err
	} else {
		if cypherBinding, hasBinding, err := extractIdentifierFromCypherExpression(pattern.Binding); err != nil {
			return nil, err
		} else if hasBinding {
			scope.Alias(cypherBinding, binding)
		}

		return binding, nil
	}
}

func (s *Translator) translateNodePattern(scope *Scope, nodePattern *cypher.NodePattern) error {
	if binding, err := s.bindNodePattern(scope, nodePattern); err != nil {
		return err
	} else if err := s.translateNodePatternToStep(scope, binding); err != nil {
		return err
	} else if len(nodePattern.Kinds) > 0 {
		if kindIDs, missingKinds := s.kindMapper.MapKinds(nodePattern.Kinds); len(missingKinds) > 0 {
			s.SetErrorf("unable to map kinds: %s", strings.Join(missingKinds.Strings(), ", "))
		} else if kindIDsLiteral, err := pgsql.AsLiteral(kindIDs); err != nil {
			s.SetError(err)
		} else {
			var (
				dependencies = pgsql.AsIdentifierSet(binding.Identifier)
				expression   = pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnKindIDs},
					pgsql.OperatorPGArrayOverlap,
					kindIDsLiteral,
				)
			)

			if err := s.treeTranslator.Constrain(dependencies, expression); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Translator) translateRelationshipPattern(scope *Scope, relationshipPattern *cypher.RelationshipPattern) error {
	if binding, err := scope.DefineNew(pgsql.EdgeComposite); err != nil {
		s.SetError(err)
	} else {
		if cypherIdentifier, hasBinding, err := extractIdentifierFromCypherExpression(relationshipPattern.Binding); err != nil {
			return err
		} else if hasBinding {
			scope.Alias(cypherIdentifier, binding)
		}

		// Apply the binding to the translation
		if err := s.translateRelationshipPatternToStep(scope, binding, relationshipPattern); err != nil {
			return err
		}

		// Capture the kind matchers for this relationship pattern
		if len(relationshipPattern.Kinds) > 0 {
			if kindIDs, missingKinds := s.kindMapper.MapKinds(relationshipPattern.Kinds); len(missingKinds) > 0 {
				s.SetErrorf("unable to map kinds: %s", strings.Join(missingKinds.Strings(), ", "))
			} else if kindIDsLiteral, err := pgsql.AsLiteral(kindIDs); err != nil {
				s.SetError(err)
			} else {
				var (
					dependencies = pgsql.AsIdentifierSet(binding.Identifier)
					expression   = pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{binding.Identifier, pgsql.ColumnKindID},
						pgsql.OperatorEquals,
						pgsql.NewAnyExpression(kindIDsLiteral),
					)
				)

				if err := s.treeTranslator.Constrain(dependencies, expression); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (s *Translator) translateRelationshipPatternToStep(scope *Scope, edgeBinding *BoundIdentifier, relationshipPattern *cypher.RelationshipPattern) error {
	var (
		part           = s.pattern.CurrentPart()
		numSteps       = len(part.TraversalSteps)
		currentStep    = part.TraversalSteps[numSteps-1]
		isContinuation = currentStep.EdgeIdentifier.Set
		nextStep       = &TraversalStep{
			EdgeIdentifier: models.ValueOptional(edgeBinding.Identifier),
			Direction:      relationshipPattern.Direction,
		}
	)

	// Look for any relationship pattern ranges. These indicate some kind of variable expansion of the path pattern.
	if relationshipPattern.Range != nil {
		// Set the edge type to an expansion of edges
		edgeBinding.DataType = pgsql.ExpansionEdge

		// Generate a new identifier to track the path expansion
		if expansionBinding, err := scope.DefineNew(pgsql.ExpansionPattern); err != nil {
			s.SetError(err)
		} else {
			// Link the edge to the expansion
			expansionBinding.Link(edgeBinding)

			if !isContinuation {
				// If this isn't a continuation then the left node was defined in isolation from the preceding node
				// pattern. Retype the left node to an expansion root node and link it to the expansion
				if leftBinding, bound := scope.Lookup(currentStep.LeftNodeIdentifier.Value); !bound {
					return fmt.Errorf("invalid identifier: %s", currentStep.LeftNodeIdentifier.Value)
				} else {
					leftBinding.DataType = pgsql.ExpansionRootNode
					expansionBinding.Link(leftBinding)
				}
			}

			if part.PatternBinding.Set {
				return fmt.Errorf("unsupported")
			}

			nextStep.Expansion = models.ValueOptional(Expansion{
				Identifier: expansionBinding.Identifier,
				MinDepth:   models.PointerOptional(relationshipPattern.Range.StartIndex),
				MaxDepth:   models.PointerOptional(relationshipPattern.Range.EndIndex),
			})
		}
	} else if part.PatternBinding.Set {
		// If there's a bound pattern track this identifier as a dependency of the pattern identifier
		part.PatternBinding.Value.DependOn(edgeBinding)
	}

	if isContinuation {
		// This is a traversal continuation so copy the right node identifier of the preceding step and then
		// add the new step
		nextStep.LeftNodeIdentifier = currentStep.RightNodeIdentifier
		part.TraversalSteps = append(part.TraversalSteps, nextStep)

		// The edge needs a constraint that ties it to the preceding edge
		if rootEdge, bound := scope.Lookup(currentStep.EdgeIdentifier.Value); !bound {
			return fmt.Errorf("invalid identifier: %s", currentStep.EdgeIdentifier.Value)
		} else if edgeConstraint, err := rightEdgeConstraint(rootEdge, edgeBinding.Identifier, nextStep.Direction); err != nil {
			return err
		} else if err := s.treeTranslator.ConstrainIdentifier(nextStep.EdgeIdentifier.Value, edgeConstraint); err != nil {
			return err
		}
	} else if leftNodeBinding, bound := scope.Lookup(currentStep.LeftNodeIdentifier.Value); !bound {
		return fmt.Errorf("invalid identifier: %s", currentStep.LeftNodeIdentifier.Value)
	} else {
		// Make the existing left node dependent on this edge's scope
		leftNodeBinding.ScopeBinding = edgeBinding

		// Carry over the left node identifier if the edge identifier for the preceding step isn't set
		nextStep.LeftNodeIdentifier = currentStep.LeftNodeIdentifier
		part.TraversalSteps[len(part.TraversalSteps)-1] = nextStep
	}

	return nil
}

func (s *Translator) translatePatternPart(scope *Scope, patternPart *cypher.PatternPart) error {
	newPatternPart := s.pattern.NewPart()

	// We expect this to be a node select if there aren't enough pattern elements for a traversal
	newPatternPart.IsTraversal = len(patternPart.PatternElements) > 1

	if cypherBinding, hasCypherSymbol, err := extractIdentifierFromCypherExpression(patternPart); err != nil {
		return err
	} else if hasCypherSymbol {
		if pathBinding, err := scope.DefineNew(pgsql.PathComposite); err != nil {
			return err
		} else {
			// Generate an alias for this binding
			scope.Alias(cypherBinding, pathBinding)

			// Record the new binding in the traversal pattern being built
			newPatternPart.PatternBinding = models.ValueOptional(pathBinding)
		}
	}

	return nil
}
