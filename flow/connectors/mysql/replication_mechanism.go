package connmysql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

type parsedReplicationOffset struct {
	mechanism string
	gset      mysql.GTIDSet
	pos       mysql.Position
}

func parseReplicationOffsetText(flavor string, offsetText string) (parsedReplicationOffset, error) {
	if rest, isFilePos := strings.CutPrefix(offsetText, "!f:"); isFilePos {
		comma := strings.LastIndexByte(rest, ',')
		if comma == -1 {
			return parsedReplicationOffset{}, fmt.Errorf("no comma in file/pos offset %s", offsetText)
		}
		offset, err := strconv.ParseUint(rest[comma+1:], 16, 32)
		if err != nil {
			return parsedReplicationOffset{}, fmt.Errorf("invalid offset in file/pos offset %s: %w", offsetText, err)
		}
		return parsedReplicationOffset{
			mechanism: protos.MySqlReplicationMechanism_MYSQL_FILEPOS.String(),
			pos:       mysql.Position{Name: rest[:comma], Pos: uint32(offset)},
		}, nil
	}

	gset, err := mysql.ParseGTIDSet(flavor, offsetText)
	if err != nil {
		return parsedReplicationOffset{}, fmt.Errorf("failed to parse mysql offset text %q as GTID set: %w", offsetText, err)
	}

	return parsedReplicationOffset{
		mechanism: protos.MySqlReplicationMechanism_MYSQL_GTID.String(),
		gset:      gset,
	}, nil
}

// ValidateCheckpointReset checks that newText is a valid offset for the source flavor and
// keeps the replication mechanism of the mirror's current checkpoint.
func ValidateCheckpointReset(flavor protos.MySqlFlavor, currentText string, newText string) error {
	if currentText == "" {
		return errors.New("mirror has no CDC checkpoint yet")
	}
	flavorName := flavorString(flavor)
	current, err := parseReplicationOffsetText(flavorName, currentText)
	if err != nil {
		return fmt.Errorf("failed to parse current checkpoint: %w", err)
	}
	next, err := parseReplicationOffsetText(flavorName, newText)
	if err != nil {
		return err
	}
	if next.mechanism == protos.MySqlReplicationMechanism_MYSQL_GTID.String() && next.gset.String() == "" {
		return errors.New("new checkpoint is an empty GTID set")
	}
	if current.mechanism != next.mechanism {
		return fmt.Errorf("new checkpoint uses %s replication but the mirror uses %s", next.mechanism, current.mechanism)
	}
	return nil
}

func replicationMechanismInUseFromOffsetText(flavor string, offsetText string) (string, error) {
	parsedOffset, err := parseReplicationOffsetText(flavor, offsetText)
	if err != nil {
		return "", err
	}

	return parsedOffset.mechanism, nil
}

func (c *MySqlConnector) GetReplicationMechanismInUse(ctx context.Context, flowJobName string) (string, error) {
	if c.config.ReplicationMechanism != protos.MySqlReplicationMechanism_MYSQL_AUTO {
		return c.config.ReplicationMechanism.String(), nil
	}
	if flowJobName == "" {
		return "", nil
	}

	lastOffset, err := c.GetLastOffset(ctx, flowJobName)
	if err != nil {
		return "", fmt.Errorf("failed to get mysql last offset for flow %s: %w", flowJobName, err)
	}

	return replicationMechanismInUseFromOffsetText(c.Flavor(), lastOffset.Text)
}
