package liteorm

import (
	"fmt"
	"reflect"
	"strings"
)

// buildCreateStatement uses reflection to build an SQL create statement based on the name and fields of the argument
// type. The argument type must be a pointer, otherwise an error is returned.
func buildCreateStatement(argt reflect.Type) (string, error) {
	tableName := BuildTableName(argt)
	sqlStatement := fmt.Sprintf("create table %s (", tableName)
	for i := 0; i < argt.NumField(); i++ {
		var columnType string
		var err error

		field := argt.Field(i)
		columnName := strings.ToLower(field.Name)
		if columnName == "id" {
			columnType = idColumnType
		} else {
			columnType, err = mapColumnType(field)
			if err != nil {
				return "", err
			}
		}

		pgsqlTag := field.Tag.Get("pgsql")
		sqlStatement += fmt.Sprintf("%s %s %s", columnName, columnType, pgsqlTag)

		// potentially add a comma, but not for the last column
		if i+1 < argt.NumField() {
			sqlStatement += ","
		}
	}

	sqlStatement += ");"

	return sqlStatement, nil
}

func buildSelectStatement(argt reflect.Type, clauses string) string {
	tableName := BuildTableName(argt)
	columnNames := ""
	for i := 0; i < argt.NumField(); i++ {
		field := argt.Field(i)
		columnNames += strings.ToLower(field.Name)

		// potentially add a comma, but not for the last column
		if i+1 < argt.NumField() {
			columnNames += ","
		}
	}

	sqlStatement := fmt.Sprintf("select %s from %s %s;", columnNames, tableName, clauses)

	return sqlStatement
}

func buildInsertStatement(argt reflect.Type) string {
	columnNames := ""
	valueIndices := ""
	nextIdx := 1
	for i := 0; i < argt.NumField(); i++ {
		field := argt.Field(i)
		if field.Name == "ID" {
			continue
		}

		columnNames += strings.ToLower(field.Name)
		valueIndices += fmt.Sprintf("$%d", nextIdx)
		nextIdx++

		// potentially add a comma, but not for the last column
		if i+1 < argt.NumField() {
			columnNames += ","
			valueIndices += ","
		}
	}

	tableName := BuildTableName(argt)
	/* the insert statement for postgresql contains a returning clause to recover the new row id
	 * https://stackoverflow.com/a/37771986
	 */
	sqlStatement := fmt.Sprintf("insert into %s (%s) values (%s) returning id;", tableName, columnNames, valueIndices)

	return sqlStatement
}

func buildUpdateStatement(argt reflect.Type, clauses string, nextIdx int) (string, int) {
	var set string
	for i := 0; i < argt.NumField(); i++ {
		field := argt.Field(i)
		if field.Name == "ID" {
			continue
		}

		set += fmt.Sprintf("%s = $%d", field.Name, nextIdx)
		nextIdx++

		// potentially add a comma, but not for the last column
		if i+1 < argt.NumField() {
			set += ","
		}
	}

	tableName := BuildTableName(argt)
	return fmt.Sprintf("update %s set %s %s;", tableName, set, clauses), nextIdx
}

func buildStatementValues(arg any) ([]any, error) {
	argv, err := getObjectValue(arg)
	if err != nil {
		return nil, err
	}

	values := make([]any, 0)
	for i := 0; i < argv.Type().NumField(); i++ {
		field := argv.Type().Field(i)
		if field.Name == "ID" {
			continue
		}

		values = append(values, argv.Field(i).Interface())
	}

	return values, nil
}

func buildDeleteStatement(argt reflect.Type, clauses string) string {
	tableName := BuildTableName(argt)
	return fmt.Sprintf("delete from %s %s;", tableName, clauses)
}

func buildTableExistsStatement(argt reflect.Type, schemaName string) string {
	tableName := BuildTableName(argt)
	return fmt.Sprintf(`
        select exists (
            select from information_schema.tables
            where table_schema = %s
            and table_name = %s
        );`, schemaName, tableName)
}
