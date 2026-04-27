// Package segment defines a flat-file format for sealed FTS indexes.
// Sealed segments are immutable: built once from an in-memory index, then
// queried by holding the file bytes plus a small term lookup table. Heap
// is dominated by the bytes themselves rather than per-node Go structs.
//
// Format:
//
//	Header (8 bytes)
//	  magic     [4]byte = "FTSE"
//	  version   uint16  = 1
//	  flags     uint16  (reserved)
//
//	Postings blob (variable)
//	  for each term, in writer order:
//	    postingCount  uvarint
//	    for each posting:
//	      ordDelta    uvarint
//	      count       uvarint
//
//	Positions blob (variable, may be empty)
//	  for each posting that has positions:
//	    positionCount  uvarint
//	    for each position:
//	      posDelta     uvarint
//
//	Term index (variable, sorted lexicographically by term)
//	  termCount uvarint
//	  for each term:
//	    termLen       uvarint
//	    termBytes     [termLen]byte
//	    postingsOff   uvarint
//	    postingsLen   uvarint
//	    positionsOff  uvarint
//	    positionsLen  uvarint
//	    hasPositions  byte (0 or 1)
//
//	Footer (24 bytes)
//	  termIndexOff  uint64
//	  termIndexLen  uint64
//	  magic         [4]byte = "FTSE"
//	  version       uint16  = 1
//	  reserved      uint16
//
// Numbers are little-endian. Uvarints follow encoding/binary.
package segment

const (
	magic   = "FTSE"
	version = uint16(1)

	headerLen = 8
	footerLen = 24
)
