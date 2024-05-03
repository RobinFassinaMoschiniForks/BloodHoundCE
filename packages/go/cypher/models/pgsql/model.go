package pgsql

import (
	"fmt"
	"github.com/specterops/bloodhound/dawgs/graph"
	"slices"
	"strings"

	"github.com/specterops/bloodhound/cypher/models"
)

type KindMapper interface {
	MapKinds(kinds graph.Kinds) ([]int16, graph.Kinds)
}

type FormattingLiteral string

func (s FormattingLiteral) AsExpression() Expression {
	return s
}

func (s FormattingLiteral) NodeType() string {
	return "formatting_literal"
}

func (s FormattingLiteral) String() string {
	return string(s)
}

// TODO: Not super happy with this syntax node name but had trouble coming up with something more appropriate
type RowShape struct {
	Columns []Identifier
}

func (s RowShape) NodeType() string {
	return "row_shape"
}

type TableAlias struct {
	Name  Identifier
	Shape models.Optional[RowShape]
}

func (s TableAlias) NodeType() string {
	return "table_alias"
}

type Values struct {
	Values []Expression
}

func (s Values) AsExpression() Expression {
	return s
}

func (s Values) AsSetExpression() SetExpression {
	return s
}

func (s Values) NodeType() string {
	return "values"
}

type Case struct {
	Operand    Expression
	Conditions []Expression
	Then       []Expression
	Else       Expression
}

// [not] exists(<query>)
type Exists struct {
	Query   Query
	Negated bool
}

// [not] in (val1, val2, ...)
type InExpression struct {
	Expression Expression
	List       []Expression
	Negated    bool
}

// [not] in (<Select> ...)
type InSubquery struct {
	Expression Expression
	Query      Query
	Negated    bool
}

// <expr> [not] between <low> and <high>
type Between struct {
	Expression Expression
	Low        Expression
	High       Expression
	Negated    bool
}

type TypeCast struct {
	Expression Expression
	CastType   DataType
}

func (s TypeCast) NodeType() string {
	return "type_cast"
}

func (s TypeCast) AsExpression() Expression {
	return s
}

func (s TypeCast) TypeHint() DataType {
	return s.CastType
}

func NewTypeCast(expression Expression, dataType DataType) TypeHinted {
	if typeCast, isTypeCast := expression.(TypeCast); isTypeCast {
		typeCast.CastType = dataType
		return typeCast
	}

	return TypeCast{
		Expression: expression,
		CastType:   dataType,
	}
}

type Literal struct {
	Value    any
	Null     bool
	CastType DataType
}

func NewLiteral(value any, dataType DataType) Literal {
	return Literal{
		Value:    value,
		CastType: dataType,
	}
}

func (s Literal) TypeHint() DataType {
	if s.CastType == UnsetDataType {
		return UnknownDataType
	}

	return s.CastType
}

func (s Literal) AsExpression() Expression {
	return s
}

func (s Literal) AsProjection() Projection {
	return s
}

func AsLiteral(value any) (Literal, error) {
	if value == nil {
		return Literal{
			Value: value,
			Null:  true,
		}, nil
	}

	if dataType, err := ValueToDataType(value); err != nil {
		return Literal{}, err
	} else {
		return Literal{
			Value:    value,
			CastType: dataType,
		}, nil
	}
}

func MustAsLiteral(value any) Literal {
	if literal, err := AsLiteral(value); err != nil {
		panic(fmt.Sprintf("%v", err))
	} else {
		return literal
	}
}

func (s Literal) NodeType() string {
	return "literal"
}

type Subquery struct {
	Query Query
}

// not <expr>
type UnaryExpression struct {
	Operator Expression
	Operand  Expression
}

func NewUnaryExpression(operator Operator, operand Expression) *UnaryExpression {
	return &UnaryExpression{
		Operator: operator,
		Operand:  operand,
	}
}

func (s UnaryExpression) AsExpression() Expression {
	return s
}

func (s UnaryExpression) NodeType() string {
	return "unary_expression"
}

type LiteralNodeValue struct {
	Value    any
	Null     bool
	CastType DataType
}

// <expr> > <expr>
// table.column > 12345
type BinaryExpression struct {
	Operator Expression
	LOperand Expression
	ROperand Expression
}

func NewBinaryExpression(left, operator, right Expression) *BinaryExpression {
	return &BinaryExpression{
		Operator: operator,
		LOperand: left,
		ROperand: right,
	}
}

func (s BinaryExpression) AsExpression() Expression {
	return s
}

func (s BinaryExpression) AsAssignment() Assignment {
	return s
}

func (s BinaryExpression) AsProjection() Projection {
	return s
}

func (s BinaryExpression) NodeType() string {
	return "binary_expression"
}

func NewPropertyLookup(identifier CompoundIdentifier, reference Literal) *BinaryExpression {
	return NewBinaryExpression(
		identifier,
		OperatorPropertyLookup,
		reference,
	)
}

type CompositeValue struct {
	Values   []Expression
	DataType DataType
}

func (s CompositeValue) NodeType() string {
	return "composite_value"
}

func (s CompositeValue) AsExpression() Expression {
	return s
}

func (s CompositeValue) AsProjection() Projection {
	return s
}

// (<expr>)
type Parenthetical struct {
	Expression Expression
}

func (s Parenthetical) NodeType() string {
	return "parenthetical"
}

func (s Parenthetical) AsExpression() Expression {
	return s
}

type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeftOuter
	JoinTypeRightOuter
	JoinTypeFullOuter
)

type JoinOperator struct {
	JoinType   JoinType
	Constraint Expression
}

type OrderBy struct {
	Expression Expression
	Ascending  bool
}

func (s OrderBy) NodeType() string {
	return "order_by"
}

type WindowFrameUnit int

const (
	WindowFrameUnitRows WindowFrameUnit = iota
	WindowFrameUnitRange
	WindowFrameUnitGroups
)

type WindowFrameBoundaryType int

const (
	WindowFrameBoundaryTypeCurrentRow WindowFrameBoundaryType = iota
	WindowFrameBoundaryTypePreceding
	WindowFrameBoundaryTypeFollowing
)

type WindowFrameBoundary struct {
	BoundaryType    WindowFrameBoundaryType
	BoundaryLiteral *Literal
}

type WindowFrame struct {
	Unit          WindowFrameUnit
	StartBoundary WindowFrameBoundary
	EndBoundary   *WindowFrameBoundary
}

type Window struct {
	PartitionBy []Expression
	OrderBy     []OrderBy
	WindowFrame *WindowFrame
}

type AllExpression struct {
	Expression
}

func NewAllExpression(inner Expression) AllExpression {
	return AllExpression{
		Expression: inner,
	}
}

type AnyExpression struct {
	Expression
	CastType DataType
}

func NewAnyExpression(inner Expression) AnyExpression {
	return AnyExpression{
		Expression: inner,
	}
}

func (s AnyExpression) AsExpression() Expression {
	return s
}

func (s AnyExpression) NodeType() string {
	return "any"
}

func (s AnyExpression) TypeHint() DataType {
	return s.CastType
}

type Parameter struct {
	Identifier Identifier
	Value      models.Optional[any]
	CastType   DataType
}

func (s Parameter) NodeType() string {
	return "parameter"
}

func (s Parameter) AsExpression() Expression {
	return s
}

func (s Parameter) TypeHint() DataType {
	if s.CastType == UnsetDataType {
		return UnknownDataType
	}

	return s.CastType
}

func AsParameter(identifier Identifier, value any) (*Parameter, error) {
	parameter := &Parameter{
		Identifier: identifier,
	}

	if value != nil {
		parameter.Value = models.ValueOptional(value)

		if dataType, err := ValueToDataType(value); err != nil {
			return parameter, err
		} else {
			parameter.CastType = dataType
		}
	}

	return parameter, nil
}

type FunctionCall struct {
	Bare       bool
	Distinct   bool
	Function   Identifier
	Parameters []Expression
	Over       *Window
	CastType   DataType
}

func (s FunctionCall) AsProjection() Projection {
	return s
}

func (s FunctionCall) AsExpression() Expression {
	return s
}

func (s FunctionCall) NodeType() string {
	return "function_call"
}

func (s FunctionCall) TypeHint() DataType {
	return s.CastType
}

type Join struct {
	Table        TableReference
	JoinOperator JoinOperator
}

func (s *Join) NodeType() string {
	return "join"
}

type Identifier string

func (s Identifier) AsProjection() Projection {
	return s
}

func (s Identifier) AsExpression() Expression {
	return s
}

func (s Identifier) NodeType() string {
	return "identifier"
}

func (s Identifier) String() string {
	return string(s)
}

func (s Identifier) Matches(others ...Identifier) bool {
	for _, other := range others {
		if s == other {
			return true
		}
	}

	return false
}

func AsOptionalIdentifier(identifier Identifier) models.Optional[Identifier] {
	return models.ValueOptional(identifier)
}

type IdentifierSet struct {
	index       map[Identifier]int
	identifiers []Identifier
}

func NewIdentifierSet() *IdentifierSet {
	return &IdentifierSet{
		index: map[Identifier]int{},
	}
}

func allocateIdentifierSet(length int) *IdentifierSet {
	return &IdentifierSet{
		index:       make(map[Identifier]int, length),
		identifiers: make([]Identifier, 0, length),
	}
}

func AsIdentifierSet(identifiers ...Identifier) *IdentifierSet {
	newSet := allocateIdentifierSet(len(identifiers))

	for _, identifier := range identifiers {
		newSet.Add(identifier)
	}

	return newSet
}

func (s *IdentifierSet) Len() int {
	return len(s.identifiers)
}

func (s *IdentifierSet) IsEmpty() bool {
	return len(s.identifiers) == 0
}

func (s *IdentifierSet) uncheckedAdd(identifier Identifier) {
	s.index[identifier] = len(s.identifiers)
	s.identifiers = append(s.identifiers, identifier)
}

func (s *IdentifierSet) Add(identifiers ...Identifier) *IdentifierSet {
	for _, identifier := range identifiers {
		if !s.Contains(identifier) {
			s.uncheckedAdd(identifier)
		}
	}

	return s
}

func (s *IdentifierSet) CheckedAdd(identifier Identifier) bool {
	if s.Contains(identifier) {
		return false
	}

	s.uncheckedAdd(identifier)
	return true
}

func (s *IdentifierSet) Copy() *IdentifierSet {
	copied := allocateIdentifierSet(len(s.identifiers))
	return copied.MergeSet(s)
}

func (s *IdentifierSet) Remove(others ...Identifier) *IdentifierSet {
	matches := make([]int, 0, len(others))

	for _, other := range others {
		if idx, hasMatch := s.index[other]; hasMatch {
			matches = append(matches, idx)
		}
	}

	if len(matches) > 0 {
		var (
			matchIdx  = 0
			compacted = make([]Identifier, 0, len(s.identifiers))
		)

		for idx := 0; idx < len(s.identifiers); idx++ {
			if matchIdx < len(matches) && idx == matches[matchIdx] {
				matchIdx += 1
				delete(s.index, s.identifiers[idx])
			} else {
				compacted = append(compacted, s.identifiers[idx])
			}
		}

		s.identifiers = compacted
	}

	return s
}

func (s *IdentifierSet) RemoveSet(other *IdentifierSet) *IdentifierSet {
	return s.Remove(other.Slice()...)
}

func (s *IdentifierSet) MergeSet(other *IdentifierSet) *IdentifierSet {
	for _, key := range other.identifiers {
		s.Add(key)
	}

	return s
}

func (s *IdentifierSet) Slice() []Identifier {
	copiedIdentifiers := make([]Identifier, len(s.identifiers))
	copy(copiedIdentifiers, s.identifiers)

	slices.Sort(copiedIdentifiers)
	return copiedIdentifiers
}

func (s *IdentifierSet) Strings() []string {
	identifierStrings := make([]string, len(s.identifiers))

	for idx, identifier := range s.identifiers {
		identifierStrings[idx] = identifier.String()
	}

	return identifierStrings
}

func (s *IdentifierSet) CombinedKey() Identifier {
	// Pull the identifiers as a sorted slice
	identifierStrings := s.Strings()
	slices.Sort(identifierStrings)

	// Join the identifiers
	return Identifier(strings.Join(identifierStrings, ""))
}

// Satifies returns true if the `other`is a subset of `s`
func (s *IdentifierSet) Satisfies(other *IdentifierSet) bool {
	for _, identifier := range other.identifiers {
		if _, satisfied := s.index[identifier]; !satisfied {
			return false
		}
	}

	return true
}

func (s *IdentifierSet) Matches(other *IdentifierSet) bool {
	return len(s.identifiers) == len(other.identifiers) && s.Satisfies(other)
}

func (s *IdentifierSet) Contains(other Identifier) bool {
	_, contained := s.index[other]
	return contained
}

type ArrayIndex struct {
	Expression Expression
	Indexes    []Expression
}

func (s ArrayIndex) NodeType() string {
	return "array_index"
}

func (s ArrayIndex) AsExpression() Expression {
	return s
}

type CompoundExpression []Expression

func (s CompoundExpression) NodeType() string {
	return "compound_expression"
}

func (s CompoundExpression) AsExpression() Expression {
	return s
}

func (s CompoundExpression) AsSlice() []Expression {
	return s
}

type CompoundIdentifier []Identifier

func (s CompoundIdentifier) Replace(old, new Identifier) {
	for idx, identifier := range s {
		if identifier == old {
			s[idx] = new
		}
	}
}

func (s CompoundIdentifier) Root() Identifier {
	return s[0]
}

func (s CompoundIdentifier) AsExpressions() []Expression {
	expressions := make([]Expression, len(s))

	for idx, identifier := range s {
		expressions[idx] = identifier.AsExpression()
	}

	return expressions
}

func (s CompoundIdentifier) Identifier() Identifier {
	return Identifier(strings.Join(s.Strings(), "."))
}

func (s CompoundIdentifier) String() string {
	return strings.Join(s.Strings(), ".")
}

func (s CompoundIdentifier) Strings() []string {
	strCopy := make([]string, len(s))

	for idx, identifier := range s {
		strCopy[idx] = identifier.String()
	}

	return strCopy
}

func (s CompoundIdentifier) IsBlank() bool {
	return len(s) == 0
}

func (s CompoundIdentifier) Copy() CompoundIdentifier {
	copyInst := make(CompoundIdentifier, len(s))
	copy(copyInst, s)

	return copyInst
}

func (s CompoundIdentifier) AsExpression() Expression {
	return s
}

func (s CompoundIdentifier) AsProjection() Projection {
	return s
}

func (s CompoundIdentifier) NodeType() string {
	return "compound_identifier"
}

type TableReference struct {
	Name    CompoundIdentifier
	Binding models.Optional[Identifier]
}

func (s TableReference) AsExpression() Expression {
	return s
}

func (s TableReference) NodeType() string {
	return "table_reference"
}

type FromClause struct {
	Relation TableReference
	Joins    []Join
}

func (s FromClause) NodeType() string {
	return "from"
}

type AliasedExpression struct {
	Expression Expression
	Alias      models.Optional[Identifier]
}

func (s AliasedExpression) NodeType() string {
	return "aliased_expression"
}

func (s AliasedExpression) AsExpression() Expression {
	return s
}

func (s AliasedExpression) AsProjection() Projection {
	return s
}

type Wildcard struct{}

func (s Wildcard) AsExpression() Expression {
	return s
}

func (s Wildcard) AsProjection() Projection {
	return s
}

func (s Wildcard) NodeType() string {
	return "wildcard"
}

type ArrayExpression struct {
	Expression Expression
}

func (s ArrayExpression) NodeType() string {
	return "array_expression"
}

func (s ArrayExpression) AsExpression() Expression {
	return s
}

type ArrayLiteral struct {
	Values   []Expression
	CastType DataType
}

func NewArrayLiteral[T any](values []T, castType DataType) (ArrayLiteral, error) {
	valuesCopy := make([]Expression, len(values))

	for idx, value := range values {
		switch typedValue := any(value).(type) {
		case Expression:
			valuesCopy[idx] = typedValue

		default:
			// Assume if it isn't an expression that it may be a bare literal and require wrapping
			if newLiteral, err := AsLiteral(value); err != nil {
				return ArrayLiteral{}, err
			} else {
				valuesCopy[idx] = newLiteral
			}
		}
	}

	return ArrayLiteral{
		Values:   valuesCopy,
		CastType: castType,
	}, nil
}

func (s ArrayLiteral) TypeHint() DataType {
	return s.CastType
}

func (s ArrayLiteral) AsExpression() Expression {
	return s
}

func (s ArrayLiteral) AsProjection() Projection {
	return s
}

func (s ArrayLiteral) NodeType() string {
	return "array"
}

type MatchedUpdate struct {
	Predicate   Expression
	Assignments []Assignment
}

func (s MatchedUpdate) NodeType() string {
	return "matched_update"
}

func (s MatchedUpdate) AsExpression() Expression {
	return s
}

func (s MatchedUpdate) AsMergeAction() MergeAction {
	return s
}

type MatchedDelete struct {
	Predicate Expression
}

func (s MatchedDelete) NodeType() string {
	return "matched_delete"
}

func (s MatchedDelete) AsExpression() Expression {
	return s
}

func (s MatchedDelete) AsMergeAction() MergeAction {
	return s
}

type UnmatchedAction struct {
	Predicate Expression
	Columns   []Identifier
	Values    Values
}

func (s UnmatchedAction) NodeType() string {
	return "unmatched_action"
}

func (s UnmatchedAction) AsExpression() Expression {
	return s
}

func (s UnmatchedAction) AsMergeAction() MergeAction {
	return s
}

type Merge struct {
	Into       bool
	Table      TableReference
	Source     TableReference
	JoinTarget Expression
	Actions    []MergeAction
}

func (s Merge) NodeType() string {
	return "merge"
}

func (s Merge) AsStatement() Statement {
	return s
}

type ConflictTarget struct {
	Columns    []Identifier
	Constraint CompoundIdentifier
}

func (s ConflictTarget) NodeType() string {
	return "conflict_target"
}

func (s ConflictTarget) AsExpression() Expression {
	return s
}

type DoNothing struct{}

type DoUpdate struct {
	Assignments []Assignment
	Where       Expression
}

func (s DoUpdate) NodeType() string {
	return "do_update"
}

func (s DoUpdate) AsExpression() Expression {
	return s
}

func (s DoUpdate) AsConflictAction() ConflictAction {
	return s
}

type OnConflict struct {
	Target *ConflictTarget
	Action ConflictAction
}

func (s OnConflict) NodeType() string {
	return "on_conflict"
}

func (s OnConflict) AsExpression() Expression {
	return s
}

type Insert struct {
	Table      CompoundIdentifier
	Columns    []Identifier
	OnConflict *OnConflict
	Source     *Query
	Returning  []Projection
}

func (s Insert) AsStatement() Statement {
	return s
}

func (s Insert) NodeType() string {
	return "insert"
}

type Delete struct {
	Table TableReference
	Where Expression
}

func (s Delete) AsStatement() Statement {
	return s
}

func (s Delete) NodeType() string {
	return "delete"
}

type Update struct {
	Table       TableReference
	Assignments []Assignment
	From        []FromClause
	Where       models.Optional[Expression]
	Returning   []Projection
}

func (s Update) AsExpression() Expression {
	return s
}

func (s Update) AsSetExpression() SetExpression {
	return s
}

func (s Update) AsStatement() Statement {
	return s
}

func (s Update) NodeType() string {
	return "update"
}

type Select struct {
	Distinct   bool
	Projection []Projection
	From       []FromClause
	Where      Expression
	GroupBy    []Expression
	Having     Expression
}

func (s Select) AsExpression() Expression {
	return s
}

func (s Select) AsSetExpression() SetExpression {
	return s
}

func (s Select) NodeType() string {
	return "select"
}

// TODO: Should this embed/compose a BinaryExpression struct?
type SetOperation struct {
	Operator Operator
	LOperand SetExpression
	ROperand SetExpression
	All      bool
	Distinct bool
}

func (s SetOperation) AsExpression() Expression {
	return s
}

func (s SetOperation) AsSetExpression() SetExpression {
	return s
}

func (s SetOperation) NodeType() string {
	return "set_operation"
}

type CommonTableExpression struct {
	Alias        TableAlias
	Materialized models.Optional[Materialized]
	Query        Query
}

func (s CommonTableExpression) NodeType() string {
	return "common_table_expression"
}

type Materialized struct {
	Materialized bool
}

func (s Materialized) AsExpression() Expression {
	return s
}

func (s Materialized) AsSetExpression() SetExpression {
	return s
}

func (s Materialized) NodeType() string {
	return "materialized"
}

type With struct {
	Recursive   bool
	Expressions []CommonTableExpression
}

func (s With) NodeType() string {
	return "with"
}

// [with <CTE>] select * from table;
type Query struct {
	CommonTableExpressions *With
	Body                   SetExpression
	OrderBy                []OrderBy
	Offset                 models.Optional[Expression]
	Limit                  models.Optional[Expression]
}

func (s Query) AddCTE(cte CommonTableExpression) {
	s.CommonTableExpressions.Expressions = append(s.CommonTableExpressions.Expressions, cte)
}

func (s Query) AsExpression() Expression {
	return s
}

func (s Query) AsSetExpression() SetExpression {
	return s
}

func (s Query) AsStatement() Statement {
	return s
}

func (s Query) NodeType() string {
	return "query"
}

func OptionalAnd(optional Expression, conjoined Expression) Expression {
	if optional == nil {
		return conjoined
	}

	return &BinaryExpression{
		Operator: OperatorAnd,
		LOperand: conjoined,
		ROperand: optional,
	}
}
