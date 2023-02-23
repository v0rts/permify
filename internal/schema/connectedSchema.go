package schema

import (
	`errors`

	`github.com/Permify/permify/pkg/dsl/utils`

	base `github.com/Permify/permify/pkg/pb/base/v1`
)

// ConnectedSchemaGraph -
type ConnectedSchemaGraph struct {
	schema *base.SchemaDefinition
}

// NewConnectedGraph -
func NewConnectedGraph(schema *base.SchemaDefinition) *ConnectedSchemaGraph {
	return &ConnectedSchemaGraph{
		schema: schema,
	}
}

type EntrypointKind string

const (
	RelationEntrypoint        EntrypointKind = "relation"
	TupleToUserSetEntrypoint  EntrypointKind = "tuple_to_user_set"
	ComputedUserSetEntrypoint EntrypointKind = "computed_user_set"
)

type Entrypoint struct {
	Kind             EntrypointKind
	Entrypoint       *base.RelationReference
	TupleSetRelation *base.RelationReference
}

// EntrypointKind is the kind of the entrypoint.
func (re Entrypoint) EntrypointKind() EntrypointKind {
	return re.Kind
}

// RelationshipEntryPoints - find the entrypoints
func (g *ConnectedSchemaGraph) RelationshipEntryPoints(target *base.RelationReference, source *base.RelationReference) ([]Entrypoint, error) {
	entries, err := g.findEntryPoint(target, source, map[string]struct{}{})
	if err != nil {
		return nil, err
	}

	return entries, nil
}

// findEntryPoint - find the entrypoint
func (g *ConnectedSchemaGraph) findEntryPoint(target *base.RelationReference, source *base.RelationReference, visited map[string]struct{}) ([]Entrypoint, error) {

	key := utils.Key(target.GetType(), target.GetRelation())
	if _, ok := visited[key]; ok {
		return nil, nil
	}
	visited[key] = struct{}{}

	def, ok := g.schema.EntityDefinitions[target.GetType()]
	if !ok {
		return nil, errors.New("entity definition not found")
	}

	if def.References[target.GetRelation()] == base.EntityDefinition_RELATIONAL_REFERENCE_ACTION {
		action, ok := def.Actions[target.GetRelation()]
		if !ok {
			return nil, nil
		}
		child := action.GetChild()
		if child.GetRewrite() != nil {
			return g.findEntryPointWithRewrite(target, source, action.GetChild().GetRewrite(), visited)
		}
		return g.findEntryPointWithLeaf(target, source, action.GetChild().GetLeaf(), visited)
	}

	return g.findRelationEntryPoint(target, source, visited)
}

// findDirectEntryPoint - find the direct entrypoint
func (g *ConnectedSchemaGraph) findRelationEntryPoint(target *base.RelationReference, source *base.RelationReference, visited map[string]struct{}) ([]Entrypoint, error) {

	var res []Entrypoint

	entity, ok := g.schema.EntityDefinitions[target.GetType()]
	if !ok {
		return nil, errors.New("entity definition not found")
	}

	relation, ok := entity.Relations[target.GetRelation()]
	if !ok {
		return nil, errors.New("relation definition not found")
	}

	if IsDirectlyRelated(relation, source) {
		res = append(res, Entrypoint{
			Kind: RelationEntrypoint,
			Entrypoint: &base.RelationReference{
				Type:     target.GetType(),
				Relation: target.GetRelation(),
			},
		})
	}

	for _, rel := range relation.GetRelationReferences() {
		if rel.GetRelation() != "" {
			entryPoints, err := g.findEntryPoint(rel, source, visited)
			if err != nil {
				return nil, err
			}
			res = append(res, entryPoints...)
		}
	}

	return res, nil
}

// findEntryPointWithLeaf - find entrypoint with leaf
func (g *ConnectedSchemaGraph) findEntryPointWithLeaf(target *base.RelationReference, source *base.RelationReference, leaf *base.Leaf, visited map[string]struct{}) ([]Entrypoint, error) {
	switch t := leaf.GetType().(type) {
	case *base.Leaf_TupleToUserSet:
		tupleSet := t.TupleToUserSet.GetTupleSet().GetRelation()
		computedUserSet := t.TupleToUserSet.GetComputed().GetRelation()

		var res []Entrypoint

		relations := g.schema.EntityDefinitions[target.GetType()].Relations[tupleSet]

		for _, rel := range relations.GetRelationReferences() {

			if rel.GetType() == source.GetType() && source.GetRelation() == computedUserSet {
				res = append(res, Entrypoint{
					Kind: TupleToUserSetEntrypoint,
					Entrypoint: &base.RelationReference{
						Type:     target.GetType(),
						Relation: target.GetRelation(),
					},
					TupleSetRelation: &base.RelationReference{
						Type:     target.GetType(),
						Relation: tupleSet,
					},
				})
			}

			subResults, err := g.findEntryPoint(
				&base.RelationReference{
					Type:     rel.GetType(),
					Relation: computedUserSet,
				},
				source,
				visited,
			)
			if err != nil {
				return nil, err
			}

			res = append(res, subResults...)
		}

		return res, nil
	case *base.Leaf_ComputedUserSet:

		if target.GetType() == source.GetType() && t.ComputedUserSet.GetRelation() == source.GetRelation() {
			return []Entrypoint{
				{
					Kind: ComputedUserSetEntrypoint,
					Entrypoint: &base.RelationReference{
						Type:     target.GetType(),
						Relation: target.GetRelation(),
					},
				},
			}, nil
		}

		return g.findEntryPoint(
			&base.RelationReference{
				Type:     target.GetType(),
				Relation: t.ComputedUserSet.GetRelation(),
			},
			source,
			visited,
		)

	default:
		return nil, errors.New("undefined leaf type")
	}
}

// findEntryPointWithRewrite - find entrypoint with rewrite
func (g *ConnectedSchemaGraph) findEntryPointWithRewrite(
	target *base.RelationReference,
	source *base.RelationReference,
	rewrite *base.Rewrite,
	visited map[string]struct{},
) ([]Entrypoint, error) {
	var err error
	var res []Entrypoint
	for _, child := range rewrite.GetChildren() {
		var results []Entrypoint
		switch child.GetType().(type) {
		case *base.Child_Rewrite:
			results, err = g.findEntryPointWithRewrite(target, source, child.GetRewrite(), visited)
			if err != nil {
				return nil, err
			}
		case *base.Child_Leaf:
			results, err = g.findEntryPointWithLeaf(target, source, child.GetLeaf(), visited)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errors.New("undefined child type")
		}
		res = append(res, results...)
	}
	return res, nil
}
