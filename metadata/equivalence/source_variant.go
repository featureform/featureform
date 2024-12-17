// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package equivalence

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/featureform/lib"
	pb "github.com/featureform/metadata/proto"
	"github.com/google/go-cmp/cmp"
)

type sourceVariant struct {
	Name       string
	Definition definition
	Provider   string
}

func SourceVariantFromProto(proto *pb.SourceVariant) (sourceVariant, error) {
	var definition definition
	switch d := proto.GetDefinition().(type) {
	case *pb.SourceVariant_PrimaryData:
		definition = primaryDataFromProto(d.PrimaryData)
	case *pb.SourceVariant_Transformation:
		transformation, err := transformationFromProto(d.Transformation)
		if err != nil {
			panic(err)
		}
		definition = transformation
	}
	return sourceVariant{
		Name:       proto.Name,
		Definition: definition,
		Provider:   proto.Provider,
	}, nil
}

func (s sourceVariant) IsEquivalent(other Equivalencer) bool {
	otherSourceVariant, ok := other.(sourceVariant)
	if !ok {
		return false
	}

	opts := cmp.Options{
		cmp.Comparer(func(d1, d2 sourceVariant) bool {
			return s.Name == otherSourceVariant.Name &&
				s.Definition.IsEquivalent(otherSourceVariant.Definition) &&
				s.Provider == otherSourceVariant.Provider
		}),
	}

	isEqual := cmp.Equal(s, otherSourceVariant, opts)

	if !isEqual {
		diff := cmp.Diff(s, otherSourceVariant, opts)
		logger.With("type", "source_variant").Debug("Unequal source variants", diff)
	}

	return isEqual
}

type definition interface {
	Equivalencer
	IsDefinition()
}

type primaryData struct {
	Location        location
	TimestampColumn string
}

func primaryDataFromProto(proto *pb.PrimaryData) primaryData {
	var location location
	switch l := proto.Location.(type) {
	case *pb.PrimaryData_Table:
		location = &sqlTable{
			Name:     l.Table.Name,
			Database: l.Table.Database,
			Schema:   l.Table.Schema,
		}
	case *pb.PrimaryData_Filestore:
		location = &fileStoreTable{
			Path: l.Filestore.Path,
		}
	case *pb.PrimaryData_Catalog:
		location = &catalogTable{
			Database:    l.Catalog.Database,
			Table:       l.Catalog.Table,
			TableFormat: l.Catalog.TableFormat,
		}
	}

	return primaryData{
		Location:        location,
		TimestampColumn: proto.TimestampColumn,
	}
}

func (p primaryData) IsDefinition() {}

func (p primaryData) IsEquivalent(other Equivalencer) bool {
	otherPrimaryData, ok := other.(primaryData)
	if !ok {
		return false
	}
	return p.Location.IsEquivalent(otherPrimaryData.Location) &&
		p.TimestampColumn == otherPrimaryData.TimestampColumn
}

type transformation struct {
	Type transformationType
	Args kubernetesArgs
}

func (t transformation) IsDefinition() {}

func transformationFromProto(proto *pb.Transformation) (transformation, error) {
	var args kubernetesArgs

	var tfType transformationType
	switch t := proto.GetType().(type) {
	case *pb.Transformation_SQLTransformation:
		tfType = sqlTransformationFromProto(t.SQLTransformation)
	case *pb.Transformation_DFTransformation:
		tfType = dfTransformationFromProto(t.DFTransformation)
	default:
		return transformation{}, fmt.Errorf("unsupported transformation type: %T", t)
	}

	args = kubernetesArgsFromProto(proto.GetKubernetesArgs())

	return transformation{
		Type: tfType,
		Args: args,
	}, nil
}

func (t transformation) IsEquivalent(other Equivalencer) bool {
	otherTransformation, ok := other.(transformation)
	if !ok {
		return false
	}

	return t.Type.IsEquivalent(otherTransformation.Type) &&
		t.Args.IsEquivalent(otherTransformation.Args)
}

type transformationType interface {
	Equivalencer
	IsTransformationType()
}

type location interface {
	Equivalencer
	IsLocationType()
}

type sqlTable struct {
	Name     string
	Database string
	Schema   string
}

func (s *sqlTable) IsLocationType() {}

func (s *sqlTable) IsEquivalent(other Equivalencer) bool {
	otherLoc, ok := other.(*sqlTable)
	if !ok {
		return false
	}
	return s.Name == otherLoc.Name &&
		s.Database == otherLoc.Database &&
		s.Schema == otherLoc.Schema
}

type fileStoreTable struct {
	Path string
}

func (f *fileStoreTable) IsLocationType() {}

func (f *fileStoreTable) IsEquivalent(other Equivalencer) bool {
	otherLoc, ok := other.(*fileStoreTable)
	if !ok {
		return false
	}
	return f.Path == otherLoc.Path
}

type catalogTable struct {
	Database    string
	Table       string
	TableFormat string
}

func (c *catalogTable) IsLocationType() {}

func (c *catalogTable) IsEquivalent(other Equivalencer) bool {
	otherLoc, ok := other.(*catalogTable)
	if !ok {
		return false
	}
	return c.Database == otherLoc.Database &&
		c.Table == otherLoc.Table &&
		c.TableFormat == otherLoc.TableFormat
}

type kubernetesArgs struct {
	image         string
	CPURequest    string
	CPULimit      string
	MemoryRequest string
	MemoryLimit   string
}

func kubernetesArgsFromProto(proto *pb.KubernetesArgs) kubernetesArgs {
	if proto == nil {
		return kubernetesArgs{}
	}
	return kubernetesArgs{
		image:         proto.DockerImage,
		CPURequest:    proto.Specs.CpuRequest,
		CPULimit:      proto.Specs.CpuLimit,
		MemoryRequest: proto.Specs.MemoryRequest,
		MemoryLimit:   proto.Specs.MemoryLimit,
	}
}

func (k kubernetesArgs) IsEquivalent(other Equivalencer) bool {
	otherArgs, ok := other.(kubernetesArgs)
	if !ok {
		return false
	}
	return k.image == otherArgs.image &&
		k.CPURequest == otherArgs.CPURequest &&
		k.CPULimit == otherArgs.CPULimit &&
		k.MemoryRequest == otherArgs.MemoryRequest &&
		k.MemoryLimit == otherArgs.MemoryLimit
}

type resourceSnowflakeConfig struct {
	DynamicTableConfig snowflakeDynamicTableConfig
	Warehouse          string
}

type snowflakeDynamicTableConfig struct {
	TargetLag   string
	RefreshMode string
	Initialize  string
}

func resourceSnowflakeConfigFromProto(proto *pb.ResourceSnowflakeConfig) resourceSnowflakeConfig {
	if proto == nil {
		return resourceSnowflakeConfig{}
	}

	return resourceSnowflakeConfig{
		DynamicTableConfig: snowflakeDynamicTableConfig{
			TargetLag:   proto.DynamicTableConfig.TargetLag,
			RefreshMode: proto.DynamicTableConfig.RefreshMode.String(),
			Initialize:  proto.DynamicTableConfig.Initialize.String(),
		},
		Warehouse: proto.Warehouse,
	}
}

func (s snowflakeDynamicTableConfig) IsEquivalent(other Equivalencer) bool {
	otherConfig, ok := other.(snowflakeDynamicTableConfig)
	if !ok {
		return false
	}
	return s.TargetLag == otherConfig.TargetLag &&
		s.RefreshMode == otherConfig.RefreshMode &&
		s.Initialize == otherConfig.Initialize
}

type sqlTransformation struct {
	Query                   string
	Sources                 []nameVariant
	IncrementalSources      []nameVariant
	ResourceSnowflakeConfig resourceSnowflakeConfig
}

func sqlTransformationFromProto(proto *pb.SQLTransformation) sqlTransformation {
	sources := nameVariantsFromProto(proto.Source)

	return sqlTransformation{
		Query:                   proto.Query,
		Sources:                 sources,
		ResourceSnowflakeConfig: resourceSnowflakeConfigFromProto(proto.ResourceSnowflakeConfig),
	}
}

func (s sqlTransformation) IsTransformationType() {}

func (s sqlTransformation) IsEquivalent(other Equivalencer) bool {
	otherSQL, ok := other.(sqlTransformation)
	if !ok {
		return false
	}

	return isSqlEqual(s.Query, otherSQL.Query) &&
		reflect.DeepEqual(s.Sources, otherSQL.Sources) &&
		reflect.DeepEqual(s.IncrementalSources, otherSQL.IncrementalSources) &&
		reflect.DeepEqual(s.ResourceSnowflakeConfig, otherSQL.ResourceSnowflakeConfig)
}

// isSqlEqual checks if two SQL strings are equal after normalizing whitespace.
func isSqlEqual(thisSql, otherSql string) bool {
	re := regexp.MustCompile(`\s+`)

	thisSql = re.ReplaceAllString(thisSql, " ")
	otherSql = re.ReplaceAllString(otherSql, " ")
	return strings.TrimSpace(thisSql) == strings.TrimSpace(otherSql)
}

type dfTransformation struct {
	Inputs             []nameVariant
	CanonicalFuncText  string
	IncrementalSources []nameVariant
}

func dfTransformationFromProto(proto *pb.DFTransformation) dfTransformation {

	inputs := make([]nameVariant, len(proto.Inputs))
	for i, input := range proto.Inputs {
		inputs[i] = nameVariant{
			Name:    input.Name,
			Variant: input.Variant,
		}
	}

	incrementalSources := make([]nameVariant, len(proto.IncrementalSources))
	for i, source := range proto.IncrementalSources {
		incrementalSources[i] = nameVariant{
			Name:    source.Name,
			Variant: source.Variant,
		}
	}

	return dfTransformation{
		Inputs:             inputs,
		CanonicalFuncText:  proto.CanonicalFuncText,
		IncrementalSources: incrementalSources,
	}
}

func (d dfTransformation) IsTransformationType() {}

func (d dfTransformation) IsEquivalent(other Equivalencer) bool {
	otherDF, ok := other.(dfTransformation)
	if !ok {
		return false
	}

	return reflect.DeepEqual(d.CanonicalFuncText, otherDF.CanonicalFuncText) &&
		reflect.DeepEqual(lib.ToSet(d.Inputs), lib.ToSet(otherDF.Inputs)) &&
		reflect.DeepEqual(lib.ToSet(d.IncrementalSources), lib.ToSet(otherDF.IncrementalSources))
}
