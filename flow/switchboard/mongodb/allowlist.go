package mongodb

import (
	"errors"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type allowedCommand struct {
	Name            string
	Required        []string
	Optional        []string
	SupportsComment bool // whether the command supports the comment field for cancel tracking
	ReturnsCursor   bool // whether the command returns a cursor (vs scalar)
	AdminDB         bool // whether the command must run against the admin database
}

var allowedCommands = map[string][]allowedCommand{
	"Query": {
		{
			Name: "find",
			Optional: []string{
				"filter", "projection", "sort", "skip", "limit", "batchSize", "singleBatch",
				"hint", "maxTimeMS", "readConcern", "collation", "allowDiskUse", "let",
			},
			SupportsComment: true, ReturnsCursor: true,
		},
		{
			Name:            "aggregate",
			Required:        []string{"pipeline"},
			Optional:        []string{"cursor", "allowDiskUse", "maxTimeMS", "readConcern", "collation", "hint", "let"},
			SupportsComment: true, ReturnsCursor: true,
		},
		{
			Name:            "count",
			Optional:        []string{"query", "limit", "skip", "hint", "maxTimeMS", "readConcern", "collation"},
			SupportsComment: true,
		},
		{
			Name:            "distinct",
			Required:        []string{"key"},
			Optional:        []string{"query", "readConcern", "collation", "hint"},
			SupportsComment: true,
		},
	},
	"User/Role Info": {
		{
			Name:            "usersInfo",
			Optional:        []string{"showCredentials", "showCustomData", "showPrivileges", "showAuthenticationRestrictions", "filter"},
			SupportsComment: true,
		},
		{
			Name:            "rolesInfo",
			Optional:        []string{"showPrivileges", "showBuiltinRoles", "showAuthenticationRestrictions"},
			SupportsComment: true,
		},
	},
	"Replication": {
		{
			Name:            "hello",
			Optional:        []string{"saslSupportedMechs"},
			SupportsComment: true,
		},
		{
			Name:            "replSetGetConfig",
			Optional:        []string{"commitmentStatus"},
			SupportsComment: true,
		},
		{Name: "replSetGetStatus"},
	},
	"Sharding": {
		{Name: "getShardMap"},
		{Name: "listShards"},
		{Name: "balancerStatus"},
		{Name: "isdbgrid"},
	},
	"Sessions": {
		{Name: "startSession"},
		{Name: "refreshSessions"},
		{Name: "killSessions"},
		{Name: "endSessions"},
		{
			Name:            "commitTransaction",
			Optional:        []string{"txnNumber", "writeConcern", "autocommit"},
			SupportsComment: true,
		},
		{
			Name:            "abortTransaction",
			Optional:        []string{"txnNumber", "writeConcern", "autocommit"},
			SupportsComment: true,
		},
	},
	"Admin": {
		{
			Name:            "listCollections",
			Optional:        []string{"filter", "nameOnly", "authorizedCollections"},
			SupportsComment: true, ReturnsCursor: true,
		},
		{
			Name:            "listDatabases",
			Optional:        []string{"filter", "nameOnly", "authorizedDatabases"},
			SupportsComment: true, AdminDB: true,
		},
		{
			Name:            "listIndexes",
			Optional:        []string{"cursor"},
			SupportsComment: true, ReturnsCursor: true,
		},
		{
			Name:            "currentOp",
			Optional:        []string{"$ownOps", "$all"},
			SupportsComment: true,
		},
		{
			Name:            "getDefaultRWConcern",
			Optional:        []string{"inMemory"},
			SupportsComment: true,
		},
		{Name: "getClusterParameter"},
		{Name: "getParameter", SupportsComment: true},
		{Name: "getCmdLineOpts"},
	},
	"Diagnostic": {
		{Name: "ping"},
		{Name: "buildInfo"},
		{
			Name:     "collStats",
			Optional: []string{"scale"},
		},
		{Name: "connPoolStats"},
		{
			Name:     "connectionStatus",
			Optional: []string{"showPrivileges"},
		},
		{
			Name:     "dataSize",
			Required: []string{"keyPattern"},
			Optional: []string{"min", "max", "estimate"},
		},
		{
			Name:     "dbStats",
			Optional: []string{"scale", "freeStorage"},
		},
		{
			Name:            "explain",
			Optional:        []string{"verbosity"},
			SupportsComment: true,
		},
		{Name: "hostInfo"},
		{Name: "listCommands"},
		{Name: "lockInfo"},
		{Name: "serverStatus"},
		{Name: "top"},
		{Name: "whatsmyuri"},
	},
}

var (
	commandLookup          map[string]allowedCommand
	commandSupportsComment map[string]struct{}
)

func init() {
	commandLookup = make(map[string]allowedCommand)
	commandSupportsComment = make(map[string]struct{})
	for _, cmds := range allowedCommands {
		for _, cmd := range cmds {
			name := strings.ToLower(cmd.Name)
			commandLookup[name] = cmd
			if cmd.SupportsComment {
				commandSupportsComment[name] = struct{}{}
			}
		}
	}
}

func isCommandAllowed(cmd string) bool {
	_, ok := commandLookup[strings.ToLower(cmd)]
	return ok
}

func lookupCommand(cmd string) (allowedCommand, bool) {
	c, ok := commandLookup[strings.ToLower(cmd)]
	return c, ok
}

// CommandSupportsComment returns whether a command supports the comment field.
func CommandSupportsComment(cmd string) bool {
	_, ok := commandSupportsComment[strings.ToLower(cmd)]
	return ok
}

func validateCommand(cmd bson.D) error {
	if len(cmd) == 0 {
		return errors.New("empty command")
	}
	if !isCommandAllowed(cmd[0].Key) {
		return fmt.Errorf("command %s is not allowed", cmd[0].Key)
	}
	return nil
}
