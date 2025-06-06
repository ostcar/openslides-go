// This tool generates the code needed for the request object.
//
// To call it, just call "go generate ./..." in the root folder of the repository
package main

import (
	"bytes"
	"cmp"
	_ "embed"
	"fmt"
	"go/format"
	"io"
	"log"
	"os"
	"slices"
	"sort"
	"strings"
	"text/template"

	"github.com/OpenSlides/openslides-go/models"
)

//go:embed value.go.tmpl
var tmplValue string

//go:embed header.go.tmpl
var tmplHeader string

//go:embed collection.go.tmpl
var tmplCollection string

func main() {
	if err := run(os.Stdout); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func run(w io.Writer) error {
	fromYml, err := parseModelsYml()
	if err != nil {
		return fmt.Errorf("parse models.yml: %w", err)
	}

	buf := new(bytes.Buffer)

	if err := genHeader(buf); err != nil {
		return fmt.Errorf("generate file header: %w", err)
	}

	if err := genValueTypes(buf); err != nil {
		return fmt.Errorf("generate value types: %w", err)
	}

	if err := genCollections(buf, fromYml); err != nil {
		return fmt.Errorf("generate collections: %w", err)
	}

	formated, err := format.Source(buf.Bytes())
	if err != nil {
		if _, err := w.Write(buf.Bytes()); err != nil {
			return fmt.Errorf("writing output: %w", err)
		}
		return fmt.Errorf("formating code: %w", err)
	}

	if _, err := w.Write(formated); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}
	return nil
}

func genHeader(buf *bytes.Buffer) error {
	tmpl, err := template.New("header.go").Parse(tmplHeader)
	if err != nil {
		return fmt.Errorf("parsing template: %w", err)
	}

	if err := tmpl.Execute(buf, nil); err != nil {
		return fmt.Errorf("executing template: %w", err)
	}

	return nil
}

var typesToGo = map[string]string{
	"ValueInt":         "int",
	"ValueMaybeInt":    "dsfetch.Maybe[int]",
	"ValueString":      "string",
	"ValueMaybeString": "dsfetch.Maybe[string]",
	"ValueBool":        "bool",
	"ValueFloat":       "float64",
	"ValueJSON":        "json.RawMessage",
	"ValueIntSlice":    "[]int",
	"ValueStringSlice": "[]string",
}

func genValueTypes(buf *bytes.Buffer) error {
	// Make sure the types are in the same order every time go generate runs.
	var types []string
	for k := range typesToGo {
		types = append(types, k)
	}
	sort.Strings(types)

	tmpl, err := template.New("value.go").Parse(tmplValue)
	if err != nil {
		return fmt.Errorf("parsing template: %w", err)
	}

	for _, name := range types {
		data := struct {
			TypeName  string
			GoType    string
			Zero      string
			MaybeType bool
		}{
			name,
			typesToGo[name],
			zeroValue(typesToGo[name]),
			strings.HasPrefix(name, "ValueMaybe"),
		}
		if err := tmpl.Execute(buf, data); err != nil {
			return fmt.Errorf("executing template: %w", err)
		}
	}
	return nil
}

// zeroValue returns the zero value for a go type
func zeroValue(t string) string {
	switch t {
	case "int", "float32":
		return "0"
	case "string":
		return `""`
	case "bool":
		return "false"
	case "json.RawMessage", "[]int", "[]string":
		return "nil"
	}
	return "unknown type " + t
}

func genCollections(buf *bytes.Buffer, fromYML map[string]models.Model) error {
	collections := toCollections(fromYML)

	tmpl, err := template.New("collection.go").Parse(tmplCollection)
	if err != nil {
		return fmt.Errorf("parsing template: %w", err)
	}

	for _, collection := range collections {
		if err := tmpl.Execute(buf, collection); err != nil {
			return fmt.Errorf("executing template: %w", err)
		}
	}

	return nil
}

func openModelYML() (io.ReadCloser, error) {
	return os.Open("../../meta/models.yml")
}

// Collection represents a models Collection for the collection.go.tmpl
type Collection struct {
	GoName     string
	GoNameLc   string
	ModelsName string
	Fields     []CollectionField
	Relations  []CollectionRelation
}

// CollectionField is one field of a Collection.
type CollectionField struct {
	Name      string
	Type      string
	FetchName string
}

// CollectionRelation is one Relation, needed for method generation.
type CollectionRelation struct {
	ResultType      string
	IsList          bool
	Type            string
	TypeLc          string
	FieldName       string
	MethodName      string
	StructFieldName string
	Required        bool
}

func toCollections(raw map[string]models.Model) []Collection {
	var collections []Collection
	for collectionName, collection := range raw {
		colGoName := goName(collectionName)
		colGoNameLc := string(colGoName[0]+32) + string(colGoName[1:])
		col := Collection{
			GoName:     colGoName,
			GoNameLc:   colGoNameLc,
			ModelsName: collectionName,
		}
		for fieldName, modelField := range collection.Fields {
			col.Fields = append(
				col.Fields,
				CollectionField{
					Name:      goName(fieldName),
					Type:      typesToGo[valueType(modelField.Type, modelField.Required)],
					FetchName: goName(collectionName) + "_" + goName(fieldName),
				},
			)

			relation := modelField.Relation()
			if relation == nil {
				continue
			}

			if strings.Contains(modelField.Type, "generic") {
				// TODO: Add generic
				//fmt.Println(collectionName, fieldName)
				continue
			}

			toType := goName(relation.ToCollections()[0].Collection)
			toTypeLc := string(toType[0]+32) + string(toType[1:])

			resultType := fmt.Sprintf("*%s", toType)
			if !relation.List() && !modelField.Required {
				resultType = fmt.Sprintf("*dsfetch.Maybe[%s]", toType)
			}
			if relation.List() {
				resultType = fmt.Sprintf("[]%s", toType)
			}

			methodName := withoutID(goName(fieldName))
			structFieldName := string(methodName[0]+32) + string(methodName[1:])

			col.Relations = append(
				col.Relations,
				CollectionRelation{
					ResultType:      resultType,
					IsList:          relation.List(),
					FieldName:       goName(fieldName),
					MethodName:      methodName,
					StructFieldName: structFieldName,
					Type:            toType,
					TypeLc:          toTypeLc,
					Required:        modelField.Required,
				},
			)

		}

		// TODO: find a way to sort in in the order defined my models.yml
		slices.SortFunc(col.Fields, func(a, b CollectionField) int {
			return cmp.Compare(a.Name, b.Name)
		})
		slices.SortFunc(col.Relations, func(a, b CollectionRelation) int {
			return cmp.Compare(a.FieldName, b.FieldName)
		})

		collections = append(collections, col)
	}

	// TODO: find a way to sort in in the order defined my models.yml
	slices.SortFunc(collections, func(a, b Collection) int {
		return cmp.Compare(a.GoName, b.GoName)
	})
	return collections
}

func parseModelsYml() (map[string]models.Model, error) {
	r, err := openModelYML()
	if err != nil {
		return nil, fmt.Errorf("open models.yml: %v", err)
	}
	defer r.Close()

	inData, err := models.Unmarshal(r)
	if err != nil {
		return nil, fmt.Errorf("unmarshalling models.yml: %w", err)
	}

	return inData, nil
}

func goName(name string) string {
	if name == "id" {
		return "ID"
	}

	name = strings.ReplaceAll(name, "_$", "")

	parts := strings.Split(name, "_")
	for i := range parts {
		parts[i] = strings.Title(parts[i])
	}
	name = strings.Join(parts, "")

	name = strings.ReplaceAll(name, "Id", "ID")
	return name
}

func withoutID(in string) string {
	result := strings.TrimSuffix(strings.TrimSuffix(in, "ID"), "IDs")

	if strings.HasSuffix(in, "IDs") {
		return result + "List"
	}
	return result
}

func valueType(modelsType string, required bool) string {
	if !required && modelsType == "relation" {
		return "ValueMaybeInt"
	}

	if !required && modelsType == "generic-relation" {
		return "ValueMaybeString"
	}

	switch modelsType {
	case "number", "relation", "timestamp":
		return "ValueInt"

	case "string", "text", "HTMLStrict", "color", "HTMLPermissive", "generic-relation", "template", "decimal(6)":
		return "ValueString"

	case "boolean":
		return "ValueBool"

	case "float":
		return "ValueFloat"

	case "relation-list", "number[]":
		return "ValueIntSlice"

	case "JSON":
		return "ValueJSON"

	case "string[]", "generic-relation-list":
		return "ValueStringSlice"

	default:
		panic(fmt.Sprintf("Unknown type %q", modelsType))
	}
}
