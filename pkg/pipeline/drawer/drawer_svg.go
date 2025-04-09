package drawer

import (
	"fmt"
	"io"
	"os"
	"sort"
	"text/template"
	"time"

	"github.com/dominikbraun/graph"
	"github.com/pkg/errors"
	"gopkg.in/go-playground/colors.v1" //nolint

	"github.com/askiada/go-pipeline/pkg/pipeline/measure"
)

// SVGDrawer is a drawer that creates a SVG file with the pipeline graph.
type SVGDrawer struct {
	graph       graph.Graph[string, string]
	steps       map[string]struct{}
	svgFileName string
}

// NewSVGDrawer creates a new SVG drawer.
func NewSVGDrawer(svgFileName string) *SVGDrawer {
	return &SVGDrawer{
		svgFileName: svgFileName,
		graph:       graph.New(graph.StringHash, graph.Directed()),
		steps:       make(map[string]struct{}),
	}
}

// AddStep adds a step to the pipeline graph.
func (d *SVGDrawer) AddStep(name string) error {
	err := d.graph.AddVertex(name)
	if err != nil {
		return errors.Wrap(err, "unable to add vertex")
	}

	d.steps[name] = struct{}{}

	return nil
}

// AddLink adds a link between parent and children steps.
func (d *SVGDrawer) AddLink(parentName, childrenName string) error {
	err := d.graph.AddEdge(parentName, childrenName)
	if err != nil {
		return errors.Wrapf(err, "unable to add edge from %s to %s", parentName, childrenName)
	}

	return nil
}

// Draw creates a SVG file with the pipeline graph.
func (d *SVGDrawer) Draw() error {
	file, err := os.Create(d.svgFileName)
	if err != nil {
		return errors.Wrapf(err, "unable to create file %s", d.svgFileName)
	}

	err = dot(d.graph, file)
	if err != nil {
		return errors.Wrapf(err, "unable to create dot file %s", d.svgFileName)
	}

	return nil
}

// SetTotalTime sets the total time for the step.
func (d *SVGDrawer) SetTotalTime(stepName string, startTime time.Time) error {
	_, properties, err := d.graph.VertexWithProperties(stepName)
	if err != nil {
		return errors.Wrap(err, "unable to get end vertex properties")
	}

	properties.Attributes["xlabel"] = time.Since(startTime).String()

	return nil
}

const maxRGB = 240

// AddMeasure adds measure to drawer.
func (d *SVGDrawer) AddMeasure(msr measure.Measure) error {
	allChanElapsed := make(map[time.Duration]string)
	sortedAllChanElapsed := []time.Duration{}

	for _, step := range msr.AllMetrics() {
		channelElapsed := step.AVGTransportDuration()
		for _, info := range channelElapsed {
			if info.Elapsed == 0 {
				continue
			}

			if _, ok := allChanElapsed[info.Elapsed]; ok {
				continue
			}

			allChanElapsed[info.Elapsed] = ""

			sortedAllChanElapsed = append(sortedAllChanElapsed, info.Elapsed)
		}
	}

	sort.Slice(sortedAllChanElapsed, func(i, j int) bool {
		return sortedAllChanElapsed[i] > sortedAllChanElapsed[j]
	})

	redColor, err := colors.RGB(255, 0, 0) //nolint
	if err != nil {
		return errors.Wrap(err, "unable to get colour")
	}

	maxValue := sortedAllChanElapsed[0]
	minValue := sortedAllChanElapsed[len(sortedAllChanElapsed)-1]

	allChanElapsed[maxValue] = redColor.ToHEX().String()
	for curr := range allChanElapsed {
		fraction := time.Duration(1)
		if maxValue > minValue {
			fraction = (curr - minValue) / (maxValue - minValue)
		}

		red := maxRGB * fraction
		blue := -maxRGB*fraction + maxRGB

		redColor, err := colors.RGB(uint8(red), 0, uint8(blue)) //nolint
		if err != nil {
			return errors.Wrap(err, "unable to get colour")
		}

		allChanElapsed[curr] = redColor.ToHEX().String()
	}

	err = d.updateMetrics(msr, allChanElapsed)
	if err != nil {
		return errors.Wrap(err, "unable to update metrics")
	}

	return nil
}

func (d *SVGDrawer) updateMetrics(msr measure.Measure, allChanElapsed map[time.Duration]string) error {
	for name, step := range msr.AllMetrics() {
		_, properties, err := d.graph.VertexWithProperties(name)
		if err != nil {
			return errors.Wrap(err, "unable to get vertex properties")
		}

		stepAvg := step.AVGDuration()
		if stepAvg != 0 {
			properties.Attributes["xlabel"] = stepAvg.String()
		}

		if step.GetTotalDuration() > 0 {
			properties.Attributes["xlabel"] += ", end: " + step.GetTotalDuration().String()
		}

		for inputStep, info := range step.AllTransports() {
			if info.Elapsed == 0 {
				continue
			}

			err := d.graph.UpdateEdge(inputStep, name,
				graph.EdgeAttribute("label", info.Elapsed.String()),
				graph.EdgeAttribute("fontcolor", "blue"),
				graph.EdgeAttribute("color", allChanElapsed[info.Elapsed]), //nolint
			)
			if err != nil {
				return errors.Wrap(err, "unable to update edge")
			}
		}
	}

	return nil
}

//nolint:lll //this is a template
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
	SourceAttributes map[string]string
	HTMLAttributes   map[string]string
	EdgeAttributes   map[string]string
	SourceWeight     int
	EdgeWeight       int
}

func dot[K comparable, T any](g graph.Graph[K, T], wrt io.Writer, options ...func(*description)) error {
	desc, err := generateDOT(g, options...)
	if err != nil {
		return fmt.Errorf("failed to generate DOT description: %w", err)
	}

	return renderDOT(wrt, desc)
}

// GraphAttribute is a functional option for the [DOT] method.
func GraphAttribute(key, value string) func(*description) {
	return func(d *description) {
		d.Attributes[key] = value
	}
}

func generateDOT[K comparable, T any](gra graph.Graph[K, T], options ...func(*description)) (description, error) {
	desc := description{
		GraphType:    "graph",
		Attributes:   make(map[string]string),
		EdgeOperator: "--",
		Statements:   make([]statement, 0),
	}

	for _, option := range options {
		option(&desc)
	}

	if gra.Traits().IsDirected {
		desc.GraphType = "digraph"
		desc.EdgeOperator = "->"
	}

	adjacencyMap, err := gra.AdjacencyMap()
	if err != nil {
		return desc, errors.Wrap(err, "unable to get adjacency map")
	}

	for vertex, adjacencies := range adjacencyMap {
		_, sourceProperties, err := gra.VertexWithProperties(vertex)
		if err != nil {
			return desc, errors.Wrap(err, "unable to get vertex properties")
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

func renderDOT(wrt io.Writer, desc description) error {
	tpl, err := template.New("dotTemplate").Parse(dotTemplate)
	if err != nil {
		return fmt.Errorf("failed to parse template: %w", err)
	}

	err = tpl.Execute(wrt, desc)
	if err != nil {
		return errors.Wrap(err, "unable to execute template")
	}

	return nil
}

var _ Drawer = (*SVGDrawer)(nil)
