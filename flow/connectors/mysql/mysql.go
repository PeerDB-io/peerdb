// stub to bypass validation

package mysql

import "context"

type MySqlConnector struct{}

func (MySqlConnector) Close() error {
	return nil
}

func (MySqlConnector) ConnectionActive(context.Context) error {
	return nil
}
