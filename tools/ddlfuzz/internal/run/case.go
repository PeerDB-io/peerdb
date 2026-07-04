package run

import "fmt"

const (
	EngineMySQL uint8 = iota
	EngineMariaDB
)

const (
	OriginGen uint8 = iota
	OriginMut
	OriginCorpus
	OriginGolden
	OriginReplay
)

type Case struct {
	SQL      []byte
	SQLMode  uint64
	Engine   uint8
	Origin   uint8
	Seed     uint64
	BaseTier uint8
	Signal   uint8
}

const (
	BaseTierFreshGen uint8 = iota
	BaseTierRecent
	BaseTierOld
)

const (
	SignalNoise uint8 = iota
	SignalBehavior
	SignalOracleEdge
)

func EngineName(engine uint8) string {
	switch engine {
	case EngineMySQL:
		return "mysql"
	case EngineMariaDB:
		return "mariadb"
	default:
		return fmt.Sprintf("unknown-%d", engine)
	}
}

func EngineID(name string) (uint8, error) {
	switch name {
	case "mysql":
		return EngineMySQL, nil
	case "mariadb", "maria":
		return EngineMariaDB, nil
	default:
		return 0, fmt.Errorf("unknown engine %q", name)
	}
}

func OriginName(origin uint8) string {
	switch origin {
	case OriginGen:
		return "gen"
	case OriginMut:
		return "mut"
	case OriginCorpus:
		return "corpus"
	case OriginGolden:
		return "golden"
	case OriginReplay:
		return "replay"
	default:
		return fmt.Sprintf("origin-%d", origin)
	}
}

func SignalName(signal uint8) string {
	switch signal {
	case SignalBehavior:
		return "behavior"
	case SignalOracleEdge:
		return "oracle-edge"
	default:
		return "noise"
	}
}

func SignalID(signal string) uint8 {
	switch signal {
	case "behavior":
		return SignalBehavior
	case "oracle-edge":
		return SignalOracleEdge
	default:
		return SignalNoise
	}
}
