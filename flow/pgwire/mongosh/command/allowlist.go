package command

import (
	"errors"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// AllowedCommand defines an allowed MongoDB wire protocol command.
type AllowedCommand struct {
	Name            string
	Required        []string
	Optional        []string
	SupportsComment bool // whether the command supports the comment field for cancel tracking
}

// AllowedCommands defines allowed MongoDB commands by category.
var AllowedCommands = map[string][]AllowedCommand{
	"Query": {
		{"find", nil, []string{
			"filter", "projection", "sort", "skip", "limit", "batchSize", "singleBatch",
			"hint", "maxTimeMS", "readConcern", "collation", "allowDiskUse", "let",
		}, true},
		{"aggregate", []string{"pipeline"}, []string{"cursor", "allowDiskUse", "maxTimeMS", "readConcern", "collation", "hint", "let"}, true},
		{"count", nil, []string{"query", "limit", "skip", "hint", "maxTimeMS", "readConcern", "collation"}, true},
		{"distinct", []string{"key"}, []string{"query", "readConcern", "collation", "hint"}, true},
		{"listIndexes", nil, []string{"cursor"}, true},
		{"listCollections", nil, []string{"filter", "nameOnly", "authorizedCollections"}, true},
		{"listDatabases", nil, []string{"filter", "nameOnly", "authorizedDatabases"}, true},
	},
	"User/Role Info": {
		{"usersInfo", nil, []string{"showCredentials", "showCustomData", "showPrivileges", "showAuthenticationRestrictions", "filter"}, true},
		{"rolesInfo", nil, []string{"showPrivileges", "showBuiltinRoles", "showAuthenticationRestrictions"}, true},
	},
	"Replication": {
		{"hello", nil, []string{"saslSupportedMechs"}, true},
		{"replSetGetConfig", nil, []string{"commitmentStatus"}, true},
		{"replSetGetStatus", nil, nil, false},
	},
	"Sharding": {
		{"getShardMap", nil, nil, false},
		{"listShards", nil, nil, false},
		{"balancerStatus", nil, nil, false},
		{"isdbgrid", nil, nil, false},
	},
	"Sessions": {
		{"startSession", nil, nil, false},
		{"refreshSessions", nil, nil, false},
		{"killSessions", nil, nil, false},
		{"endSessions", nil, nil, false},
		{"commitTransaction", nil, []string{"txnNumber", "writeConcern", "autocommit"}, true},
		{"abortTransaction", nil, []string{"txnNumber", "writeConcern", "autocommit"}, true},
	},
	"Admin": {
		{"currentOp", nil, []string{"$ownOps", "$all"}, true},
		{"getDefaultRWConcern", nil, []string{"inMemory"}, true},
		{"getClusterParameter", nil, nil, false},
		{"getParameter", nil, nil, true},
		{"getCmdLineOpts", nil, nil, false},
	},
	"Diagnostic": {
		{"ping", nil, nil, false},
		{"buildInfo", nil, nil, false},
		{"collStats", nil, []string{"scale"}, false},
		{"connPoolStats", nil, nil, false},
		{"connectionStatus", nil, []string{"showPrivileges"}, false},
		{"dataSize", []string{"keyPattern"}, []string{"min", "max", "estimate"}, false},
		{"dbStats", nil, []string{"scale", "freeStorage"}, false},
		{"explain", nil, []string{"verbosity"}, true},
		{"hostInfo", nil, nil, false},
		{"listCommands", nil, nil, false},
		{"lockInfo", nil, nil, false},
		{"serverStatus", nil, nil, false},
		{"top", nil, nil, false},
		{"whatsmyuri", nil, nil, false},
	},
}

var (
	allowedCommandsSet     map[string]struct{}
	commandSupportsComment map[string]struct{}
)

func init() {
	allowedCommandsSet = make(map[string]struct{})
	commandSupportsComment = make(map[string]struct{})
	for _, cmds := range AllowedCommands {
		for _, cmd := range cmds {
			allowedCommandsSet[cmd.Name] = struct{}{}
			if cmd.SupportsComment {
				commandSupportsComment[cmd.Name] = struct{}{}
			}
		}
	}
}

// IsCommandAllowed returns whether a command is in the allow list.
func IsCommandAllowed(cmd string) bool {
	_, ok := allowedCommandsSet[cmd]
	return ok
}

// CommandSupportsComment returns whether a command supports the comment field.
func CommandSupportsComment(cmd string) bool {
	_, ok := commandSupportsComment[cmd]
	return ok
}

// ValidateCommand validates a built command against the allow list.
func ValidateCommand(cmd bson.D) error {
	if len(cmd) == 0 {
		return errors.New("empty command")
	}
	if !IsCommandAllowed(cmd[0].Key) {
		return fmt.Errorf("command %s is not allowed", cmd[0].Key)
	}
	return nil
}

// ValidateInput validates input constraints.
func ValidateInput(input string) error {
	return checkMultipleStatements(input)
}

func checkMultipleStatements(input string) error {
	inString := false
	stringChar := byte(0)
	escaped := false

	for i := range len(input) {
		ch := input[i]

		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}

		if !inString {
			switch ch {
			case '"', '\'', '`':
				inString = true
				stringChar = ch
			case ';':
				remaining := strings.TrimSpace(input[i+1:])
				if remaining != "" {
					return errors.New("multiple statements not supported")
				}
			}
		} else if ch == stringChar {
			inString = false
			stringChar = 0
		}
	}

	return nil
}
