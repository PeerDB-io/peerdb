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
		{"listindexes", nil, []string{"cursor"}, true},
		{"listcollections", nil, []string{"filter", "nameOnly", "authorizedCollections"}, true},
		{"listdatabases", nil, []string{"filter", "nameOnly", "authorizedDatabases"}, true},
	},
	"User/Role Info": {
		{"usersinfo", nil, []string{"showCredentials", "showCustomData", "showPrivileges", "showAuthenticationRestrictions", "filter"}, true},
		{"rolesinfo", nil, []string{"showPrivileges", "showBuiltinRoles", "showAuthenticationRestrictions"}, true},
	},
	"Replication": {
		{"hello", nil, []string{"saslSupportedMechs"}, true},
		{"replsetgetconfig", nil, []string{"commitmentStatus"}, true},
		{"replsetgetstatus", nil, nil, false},
	},
	"Sharding": {
		{"getshardmap", nil, nil, false},
		{"listshards", nil, nil, false},
		{"balancerstatus", nil, nil, false},
		{"isdbgrid", nil, nil, false},
	},
	"Sessions": {
		{"startsession", nil, nil, false},
		{"refreshsessions", nil, nil, false},
		{"killsessions", nil, nil, false},
		{"endsessions", nil, nil, false},
		{"committransaction", nil, []string{"txnNumber", "writeConcern", "autocommit"}, true},
		{"aborttransaction", nil, []string{"txnNumber", "writeConcern", "autocommit"}, true},
	},
	"Admin": {
		{"currentop", nil, []string{"$ownOps", "$all"}, true},
		{"getdefaultrwconcern", nil, []string{"inMemory"}, true},
		{"getclusterparameter", nil, nil, false},
		{"getparameter", nil, nil, true},
		{"getcmdlineopts", nil, nil, false},
	},
	"Diagnostic": {
		{"ping", nil, nil, false},
		{"buildinfo", nil, nil, false},
		{"collstats", nil, []string{"scale"}, false},
		{"connpoolstats", nil, nil, false},
		{"connectionstatus", nil, []string{"showPrivileges"}, false},
		{"datasize", []string{"keyPattern"}, []string{"min", "max", "estimate"}, false},
		{"dbstats", nil, []string{"scale", "freeStorage"}, false},
		{"explain", nil, []string{"verbosity"}, true},
		{"hostinfo", nil, nil, false},
		{"listcommands", nil, nil, false},
		{"lockinfo", nil, nil, false},
		{"serverstatus", nil, nil, false},
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
			name := strings.ToLower(cmd.Name)
			allowedCommandsSet[name] = struct{}{}
			if cmd.SupportsComment {
				commandSupportsComment[name] = struct{}{}
			}
		}
	}
}

// IsCommandAllowed returns whether a command is in the allow list.
func IsCommandAllowed(cmd string) bool {
	_, ok := allowedCommandsSet[strings.ToLower(cmd)]
	return ok
}

// CommandSupportsComment returns whether a command supports the comment field.
func CommandSupportsComment(cmd string) bool {
	_, ok := commandSupportsComment[strings.ToLower(cmd)]
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
		} else {
			if ch == stringChar {
				inString = false
				stringChar = 0
			}
		}
	}

	return nil
}
