// This tool generates the list of related fields in the file def.go.
// To call it, just call "go generate ./..." in the root folder of the repository
package main

import (
	"bytes"
	"fmt"
	"go/format"
	"io"
	"log"
	"os"
	"sort"
	"text/template"

	"github.com/OpenSlides/openslides-go/models"
)

func main() {
	r, err := openModelYML()
	if err != nil {
		log.Fatalf("Can not load models defition: %v", err)
	}
	defer r.Close()

	td, err := parse(r)
	if err != nil {
		log.Fatalf("Can not parse model definition: %v", err)
	}

	if err := writeFile(os.Stdout, td); err != nil {
		log.Fatalf("Can not write result: %v", err)
	}
}

func openModelYML() (io.ReadCloser, error) {
	return os.Open("../meta/models.yml")
}

type restriction struct {
	Collection string
	Field      string
	Mode       string
}

func (r restriction) CollectionField() string {
	collectionField := fmt.Sprintf("%s/%s", r.Collection, r.Field)
	return collectionField
}

type templateData struct {
	Relation            map[string]string
	RelationList        map[string]string
	GenericRelation     map[string]map[string]string
	GenericRelationList map[string]map[string]string
	Restrictions        map[string][]restriction
}

// parse returns all relation-list and generic-relation-list fields and where
// they point to.
func parse(r io.Reader) (td templateData, err error) {
	inData, err := models.Unmarshal(r)
	if err != nil {
		return td, fmt.Errorf("unmarshalling models.yml: %w", err)
	}

	td.Relation = make(map[string]string)
	td.RelationList = make(map[string]string)
	td.GenericRelation = make(map[string]map[string]string)
	td.GenericRelationList = make(map[string]map[string]string)
	td.Restrictions = make(map[string][]restriction)
	for modelName, model := range inData {
		for fieldName, field := range model.Fields {
			collectionField := fmt.Sprintf("%s/%s", modelName, fieldName)
			td.Restrictions[modelName] = append(td.Restrictions[modelName], restriction{Collection: modelName, Field: fieldName, Mode: field.RestrictionMode()})

			relation := field.Relation()

			if relation == nil {
				continue
			}

			switch v := relation.(type) {
			case *models.AttributeRelation:
				to := v.ToCollections()[0].Collection + "/" + v.ToCollections()[0].ToField.Name
				if relation.List() {
					td.RelationList[collectionField] = to
				} else {
					td.Relation[collectionField] = to
				}

			case *models.AttributeGenericRelation:
				fields := make(map[string]string)
				for _, toField := range v.ToCollections() {
					fields[toField.Collection] = toField.ToField.Name
				}
				if relation.List() {
					td.GenericRelationList[collectionField] = fields
				} else {
					td.GenericRelation[collectionField] = fields
				}

			default:
				return td, fmt.Errorf("unknown type %t for field.Relation", v)
			}

		}

		sort.Slice(td.Restrictions[modelName], func(i, j int) bool {
			if td.Restrictions[modelName][i].Mode == td.Restrictions[modelName][j].Mode {
				return td.Restrictions[modelName][i].CollectionField() < td.Restrictions[modelName][j].CollectionField()
			}
			return td.Restrictions[modelName][i].Mode < td.Restrictions[modelName][j].Mode
		})
	}

	return td, nil
}

const tpl = `// Code generated with models.yml DO NOT EDIT.
package metagen

// RelatoinFields is a map from are all (single) relation fields to the fields,
// there relate to.
var RelationFields = map[string]string{
	{{- range $key, $value := .Relation}}
		"{{$key}}": "{{$value}}",
	{{- end}}
}

// RelationListFields is a map from all relation list fields to the fields,
// there relate to.
var RelationListFields = map[string]string{
	{{- range $key, $value := .RelationList}}
		"{{$key}}": "{{$value}}",
	{{- end}}
}

// GenericRelationFields is a map from are all (single) generic relation fields
// to the fields, there relate to.
var GenericRelationFields = map[string]map[string]string{
	{{- range $key, $value := .GenericRelation}}
		"{{$key}}": { {{range $innerKey, $innerValue := $value}} "{{$innerKey}}": "{{$innerValue}}", {{end}} },
	{{- end}}
}

// GenericRelationListFields is a map from all generic relation list fields to
// the fields, there relate to.
var GenericRelationListFields = map[string]map[string]string{
	{{- range $key, $value := .GenericRelationList}}
		"{{$key}}": { {{range $innerKey, $innerValue := $value}} "{{$innerKey}}": "{{$innerValue}}", {{end}} },
	{{- end}}
}

// RestrictionModes are all fields to there restriction_mode.
var RestrictionModes = map[string]string{
	{{- range $modelName, $model := .Restrictions}}
		// {{$modelName}}
		{{- range $field := $model}}
			"{{$field.CollectionField}}": "{{$field.Mode}}",
		{{- end}}
	{{end}}
}
`

func writeFile(w io.Writer, td templateData) error {
	t := template.New("t")
	t, err := t.Parse(tpl)
	if err != nil {
		return fmt.Errorf("parsing template: %w", err)
	}

	buf := new(bytes.Buffer)

	if err := t.Execute(buf, td); err != nil {
		return fmt.Errorf("writing template: %w", err)
	}

	formated, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("formating code: %w", err)
	}

	if _, err := w.Write(formated); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}
	return nil
}
