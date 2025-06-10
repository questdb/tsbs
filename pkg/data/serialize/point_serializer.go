package serialize

import (
	"io"

	"github.com/questdb/tsbs/pkg/data"
)

// PointSerializer serializes a Point for writing
type PointSerializer interface {
	Serialize(p *data.Point, w io.Writer) error
}
