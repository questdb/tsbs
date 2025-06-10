package iot

import (
	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/questdb/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/questdb/tsbs/pkg/query"
)

// StationaryTrucks contains info for filling in stationary trucks queries.
type StationaryTrucks struct {
	core utils.QueryGenerator
}

// NewStationaryTrucks creates a new stationary trucks query filler.
func NewStationaryTrucks(core utils.QueryGenerator) utils.QueryFiller {
	return &StationaryTrucks{
		core: core,
	}
}

// Fill fills in the query.Query with query details.
func (i *StationaryTrucks) Fill(q query.Query) query.Query {
	fc, ok := i.core.(StationaryTrucksFiller)
	if !ok {
		common.PanicUnimplementedQuery(i.core)
	}
	fc.StationaryTrucks(q)
	return q
}
