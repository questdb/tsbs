package questdb

// The QWP data file is a binary, schema-and-dictionary encoded form of the
// same points the ILP text format carries. It exists so that the loader can
// feed the QuestDB wire protocol without re-parsing text: tag and column
// names arrive as dictionary ids the loader resolves to strings once, and
// numeric values arrive in their native width instead of as decimal digits.
//
// Layout, little-endian throughout:
//
//	"QWPB" <version:uint8>
//	then a sequence of records, each introduced by a one-byte kind:
//
//	  QwpRecString  uvarint len, len bytes
//	      Defines the next string id. Ids are assigned sequentially from
//	      zero in the order the definitions appear.
//
//	  QwpRecSchema  uvarint tableStrId,
//	                uvarint tagCount,   tagCount * uvarint keyStrId,
//	                uvarint fieldCount, fieldCount * (uvarint keyStrId, uint8 type)
//	      Defines the next schema id, again sequential from zero. A schema
//	      is the shape of a row: its table, its symbol columns and its
//	      typed columns. Points of one measurement that omit a null tag or
//	      field simply define a second schema.
//
//	  QwpRecRow     uvarint schemaId, uvarint payloadLen, payload
//	      payload: tagCount * uvarint valueStrId,
//	               fieldCount * value encoded per its column type,
//	               int64 timestamp in nanoseconds since the epoch.
//
// The row payload carries its length so that a reader can slice a row out
// of the stream without consulting its schema.
const (
	// QwpMagic introduces a QWP data file.
	QwpMagic = "QWPB"
	// QwpVersion is the format version this package reads and writes.
	QwpVersion = 1
)

// Record kinds.
const (
	QwpRecString = 0x01
	QwpRecSchema = 0x02
	QwpRecRow    = 0x03
)

// Column type codes, as stored in a schema definition.
const (
	QwpTypeInt64   = 0x01 // 8 bytes, little-endian
	QwpTypeFloat64 = 0x02 // 8 bytes, IEEE-754 bits, little-endian
	QwpTypeBool    = 0x03 // 1 byte, 0 or 1
	QwpTypeString  = 0x04 // uvarint length, then the bytes
)
