package pipeline

import (
	"fmt"
	"io"
	"os"
	"text/template"

	"github.com/dominikbraun/graph"
)

type drawer struct {
	svgFileName string
	graph       graph.Graph[string, string]
	steps       map[string]struct{}
}

func newDrawer(svgFileName string) *drawer {
	return &drawer{
		svgFileName: svgFileName,
		graph:       graph.New(graph.StringHash, graph.Directed()),
		steps:       make(map[string]struct{}),
	}
}

func (d *drawer) addStep(name string) error {
	err := d.graph.AddVertex(name)
	if err != nil {
		return err
	}
	d.steps[name] = struct{}{}
	return nil
}

func (d *drawer) addLink(parentName, childrenName string) error {
	err := d.graph.AddEdge(parentName, childrenName)
	if err != nil {
		return err
	}
	return nil
}

func (d *drawer) draw() error {
	file, err := os.Create(d.svgFileName)
	if err != nil {
		return err
	}
	err = dot(d.graph, file)
	if err != nil {
		return err
	}
	return nil
}

// ToDo: This template should be simplified and split into multiple templates.
const dotTemplate = `strict {{.GraphType}} {
	{{range $k, $v := .Attributes}}
		{{$k}}="{{$v}}";
	{{end}}
	{{range $s := .Statements}}
		"{{.Source}}" {{if .Target}}{{$.EdgeOperator}} "{{.Target}}" [ {{range $k, $v := .EdgeAttributes}}{{$k}}="{{$v}}", {{end}} weight={{.EdgeWeight}} ]{{else}}[ {{range $k, $v := .HTMLAttributes}}{{$k}}={{$v}}, {{end}} {{range $k, $v := .SourceAttributes}}{{$k}}="{{$v}}", {{end}} weight={{.SourceWeight}} ]{{end}};
	{{end}}
	}
	`

type description struct {
	GraphType    string
	Attributes   map[string]string
	EdgeOperator string
	Statements   []statement
}

type statement struct {
	Source           interface{}
	Target           interface{}
	SourceWeight     int
	SourceAttributes map[string]string
	HTMLAttributes   map[string]string
	EdgeWeight       int
	EdgeAttributes   map[string]string
}

func dot[K comparable, T any](g graph.Graph[K, T], w io.Writer, options ...func(*description)) error {
	desc, err := generateDOT(g, options...)
	if err != nil {
		return fmt.Errorf("failed to generate DOT description: %w", err)
	}

	return renderDOT(w, desc)
}

// GraphAttribute is a functional option for the [DOT] method.
func GraphAttribute(key, value string) func(*description) {
	return func(d *description) {
		d.Attributes[key] = value
	}
}

func generateDOT[K comparable, T any](g graph.Graph[K, T], options ...func(*description)) (description, error) {
	desc := description{
		GraphType:    "graph",
		Attributes:   make(map[string]string),
		EdgeOperator: "--",
		Statements:   make([]statement, 0),
	}

	for _, option := range options {
		option(&desc)
	}

	if g.Traits().IsDirected {
		desc.GraphType = "digraph"
		desc.EdgeOperator = "->"
	}

	adjacencyMap, err := g.AdjacencyMap()
	if err != nil {
		return desc, err
	}

	for vertex, adjacencies := range adjacencyMap {
		_, sourceProperties, err := g.VertexWithProperties(vertex)
		if err != nil {
			return desc, err
		}
		htmlAttributes := make(map[string]string)
		if xlabel, ok := sourceProperties.Attributes["xlabel"]; ok {
			htmlAttributes["label"] = fmt.Sprintf(`<%+v <BR /> <FONT POINT-SIZE="12">%s</FONT>>`, vertex, xlabel)
			delete(sourceProperties.Attributes, "xlabel")
		}

		stmt := statement{
			Source:           vertex,
			SourceWeight:     sourceProperties.Weight,
			SourceAttributes: sourceProperties.Attributes,
			HTMLAttributes:   htmlAttributes,
		}
		desc.Statements = append(desc.Statements, stmt)

		for adjacency, edge := range adjacencies {
			stmt := statement{
				Source:         vertex,
				Target:         adjacency,
				EdgeWeight:     edge.Properties.Weight,
				EdgeAttributes: edge.Properties.Attributes,
			}
			desc.Statements = append(desc.Statements, stmt)
		}
	}

	return desc, nil
}

func renderDOT(w io.Writer, d description) error {
	tpl, err := template.New("dotTemplate").Parse(dotTemplate)
	if err != nil {
		return fmt.Errorf("failed to parse template: %w", err)
	}

	return tpl.Execute(w, d)
}
