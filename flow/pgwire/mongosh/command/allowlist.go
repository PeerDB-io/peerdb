package command

import (
	"errors"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// AllowedCommand defines an allowed MongoDB wire protocol command.
type AllowedCommand struct {
	Name     string
	Required []string
	Optional []string
}

// AllowedCommands defines allowed MongoDB commands by category.
var AllowedCommands = map[string][]AllowedCommand{
	"Query": {
		{"find", nil, []string{
			"filter", "projection", "sort", "skip", "limit", "batchSize", "singleBatch",
			"hint", "maxTimeMS", "readConcern", "collation", "allowDiskUse", "let",
		}},
		{"aggregate", []string{"pipeline"}, []string{"cursor", "allowDiskUse", "maxTimeMS", "readConcern", "collation", "hint", "let"}},
		{"count", nil, []string{"query", "limit", "skip", "hint", "maxTimeMS", "readConcern", "collation"}},
		{"distinct", []string{"key"}, []string{"query", "readConcern", "collation", "hint"}},
		{"listindexes", nil, []string{"cursor"}},
		{"listcollections", nil, []string{"filter", "nameOnly", "authorizedCollections"}},
		{"listdatabases", nil, []string{"filter", "nameOnly", "authorizedDatabases"}},
	},
	"User/Role Info": {
		{"usersinfo", nil, []string{"showCredentials", "showCustomData", "showPrivileges", "showAuthenticationRestrictions", "filter"}},
		{"rolesinfo", nil, []string{"showPrivileges", "showBuiltinRoles", "showAuthenticationRestrictions"}},
	},
	"Replication": {
		{"hello", nil, []string{"saslSupportedMechs"}},
		{"replsetgetconfig", nil, []string{"commitmentStatus"}},
		{"replsetgetstatus", nil, nil},
	},
	"Sharding": {
		{"getshardmap", nil, nil},
		{"listshards", nil, nil},
		{"balancerstatus", nil, nil},
		{"isdbgrid", nil, nil},
	},
	"Sessions": {
		{"startsession", nil, nil},
		{"refreshsessions", nil, nil},
		{"killsessions", nil, nil},
		{"endsessions", nil, nil},
		{"committransaction", nil, []string{"txnNumber", "writeConcern", "autocommit"}},
		{"aborttransaction", nil, []string{"txnNumber", "writeConcern", "autocommit"}},
	},
	"Admin": {
		{"currentop", nil, []string{"$ownOps", "$all"}},
		{"getdefaultrwconcern", nil, []string{"inMemory"}},
		{"getclusterparameter", nil, nil},
		{"getparameter", nil, nil},
		{"getcmdlineopts", nil, nil},
	},
	"Diagnostic": {
		{"ping", nil, nil},
		{"buildinfo", nil, nil},
		{"collstats", nil, []string{"scale"}},
		{"connpoolstats", nil, nil},
		{"connectionstatus", nil, []string{"showPrivileges"}},
		{"datasize", []string{"keyPattern"}, []string{"min", "max", "estimate"}},
		{"dbstats", nil, []string{"scale", "freeStorage"}},
		{"explain", nil, []string{"verbosity"}},
		{"hostinfo", nil, nil},
		{"listcommands", nil, nil},
		{"lockinfo", nil, nil},
		{"serverstatus", nil, nil},
		{"top", nil, nil},
		{"whatsmyuri", nil, nil},
	},
}

var allowedCommandsSet map[string]bool

func init() {
	allowedCommandsSet = make(map[string]bool)
	for _, cmds := range AllowedCommands {
		for _, cmd := range cmds {
			allowedCommandsSet[strings.ToLower(cmd.Name)] = true
		}
	}
}

// IsCommandAllowed returns whether a command is in the allow list.
func IsCommandAllowed(cmd string) bool {
	return allowedCommandsSet[strings.ToLower(cmd)]
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
			if ch == '"' || ch == '\'' || ch == '`' {
				inString = true
				stringChar = ch
			} else if ch == ';' {
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
