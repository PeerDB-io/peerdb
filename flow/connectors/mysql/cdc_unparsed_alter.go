package connmysql

import (
	"regexp"
	"strings"

	"github.com/PeerDB-io/peerdb/flow/model"
)

const mysqlIdentifierPattern = "(?:`(?:``|[^`])*`|[0-9A-Za-z_$]+)"

var (
	mysqlAlterTableStatementRe = regexp.MustCompile(
		`(?is)(?:^|;)\s*alter\s+(?:online\s+|offline\s+|ignore\s+)?table\s+(` +
			mysqlIdentifierPattern + `)(?:\s*\.\s*(` + mysqlIdentifierPattern + `))?\s+([^;]*)`)
	mysqlAlterSpecActionRe = regexp.MustCompile(`(?is)(?:^|,)\s*(add|drop|modify|change|alter)\b\s*([^,]*)`)
)

var mysqlNonColumnAlterTargets = map[string]struct{}{
	"check":      {},
	"columnar":   {},
	"constraint": {},
	"foreign":    {},
	"fulltext":   {},
	"index":      {},
	"key":        {},
	"partition":  {},
	"primary":    {},
	"spatial":    {},
	"unique":     {},
	"vector":     {},
}

func mappedTableForUnparsedColumnAlter(
	rawSQL string,
	eventSchema string,
	tableNameMapping map[string]model.NameAndExclude,
) (string, bool) {
	for _, match := range mysqlAlterTableStatementRe.FindAllStringSubmatch(rawSQL, -1) {
		if !hasColumnAlterAction(match[3]) {
			continue
		}

		schemaName := strings.TrimSpace(eventSchema)
		tableName := mysqlUnquoteIdentifier(match[1])
		if match[2] != "" {
			schemaName = mysqlUnquoteIdentifier(match[1])
			tableName = mysqlUnquoteIdentifier(match[2])
		}
		if schemaName == "" || tableName == "" {
			continue
		}

		sourceTableName := schemaName + "." + tableName
		if tableNameMapping[sourceTableName].Name != "" {
			return sourceTableName, true
		}
	}
	return "", false
}

func hasColumnAlterAction(alterClauses string) bool {
	for _, match := range mysqlAlterSpecActionRe.FindAllStringSubmatch(alterClauses, -1) {
		action := strings.ToLower(match[1])
		rest := strings.TrimSpace(match[2])
		switch action {
		case "modify", "change":
			return true
		case "alter":
			if strings.HasPrefix(strings.ToLower(rest), "column ") {
				return true
			}
		case "add", "drop":
			rest = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(rest), "column "))
			if firstToken, _, ok := strings.Cut(rest, " "); ok {
				_, nonColumn := mysqlNonColumnAlterTargets[firstToken]
				if !nonColumn {
					return true
				}
			} else if rest != "" {
				_, nonColumn := mysqlNonColumnAlterTargets[rest]
				if !nonColumn {
					return true
				}
			}
		}
	}
	return false
}

func mysqlUnquoteIdentifier(identifier string) string {
	identifier = strings.TrimSpace(identifier)
	if len(identifier) >= 2 && identifier[0] == '`' && identifier[len(identifier)-1] == '`' {
		return strings.ReplaceAll(identifier[1:len(identifier)-1], "``", "`")
	}
	return identifier
}
