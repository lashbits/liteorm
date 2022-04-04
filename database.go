package liteorm

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"reflect"
)

type Database struct {
	Conn *pgx.Conn
}

func NewDatabase(connString string) (*Database, error) {
	conn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		return nil, err
	}

	db := &Database{
		Conn: conn,
	}

	return db, nil
}

func (db *Database) Close() {
	db.Conn.Close(context.Background())
}

func (db *Database) CreateTable(t reflect.Type, dropExisting bool) error {
	tableName := BuildTableName(t)
	errmsg := fmt.Sprintf("could not create table %s", tableName)

	if dropExisting {
		statement := fmt.Sprintf("drop table if exists %s cascade;", tableName)
		_, err := db.Conn.Exec(context.Background(), statement)
		if err != nil {
			return errors.Wrap(err, errmsg)
		}
	}

	statement, err := buildCreateStatement(t)
	if err != nil {
		return errors.Wrap(err, errmsg)
	}

	_, err = db.Conn.Exec(context.Background(), statement)
	if err != nil {
		return errors.Wrap(err, errmsg)
	}

	return nil
}

func (db *Database) Insert(arg any) error {
	var lastID int64

	argt, err := getObjectType(arg)
	if err != nil {
		return errors.Wrap(err, "could not insert object")
	}
	errmsg := fmt.Sprintf("could not insert object of type %s", argt.Name())

	statement := buildInsertStatement(argt)
	values, err := buildStatementValues(arg)
	if err != nil {
		return errors.Wrap(err, "could not insert object")
	}

	err = db.Conn.QueryRow(context.Background(), statement, values...).Scan(&lastID)
	if err != nil {
		return errors.Wrap(err, errmsg)
	}

	setIDValue(arg, lastID)
	if err != nil {
		return errors.Wrap(err, errmsg)
	}

	return nil
}

func (db *Database) SelectOne(arg any, clauses string, args ...any) error {
	argt, err := getObjectType(arg)
	if err != nil {
		return errors.Wrap(err, "could not select object")
	}

	errmsg := fmt.Sprintf("could not select object of type %s", argt.Name())

	statement := buildSelectStatement(argt, clauses)
	row := db.Conn.QueryRow(context.Background(), statement, args...)

	columnValues := buildSliceFromFields(argt)
	err = row.Scan(columnValues...)
	if err != nil {
		return errors.Wrap(err, errmsg)
	}

	err = setObjectFields(arg, columnValues...)
	if err != nil {
		return errors.Wrap(err, errmsg)
	}

	return nil
}

func (db *Database) Select(t reflect.Type, clauses string, args ...any) (any, error) {
	errmsg := fmt.Sprintf("could not select objects of type %s", t.Name())

	statement := buildSelectStatement(t, clauses)
	rows, err := db.Conn.Query(context.Background(), statement, args...)
	defer rows.Close()
	if err != nil {
		return nil, errors.Wrap(err, errmsg)
	}

	result := reflect.MakeSlice(reflect.SliceOf(t), 0, 0)
	for rows.Next() {
		columnValues := buildSliceFromFields(t)
		err = rows.Scan(columnValues...)
		if err != nil {
			return nil, errors.Wrap(err, errmsg)
		}

		newelem := reflect.New(t).Interface()

		err = setObjectFields(newelem, columnValues...)
		if err != nil {
			return nil, errors.Wrap(err, errmsg)
		}

		result = reflect.Append(result, reflect.ValueOf(newelem).Elem())
	}

	return result.Interface(), nil
}

func (db *Database) UpdateOne(arg any) error {
	argt, err := getObjectType(arg)
	if err != nil {
		return errors.Wrap(err, "could not update object")
	}

	errmsg := fmt.Sprintf("could not update object of type %s", argt.Name())

	statement, _ := buildUpdateStatement(argt, "where id = $1", 2)
	values, err := buildStatementValues(arg)
	if err != nil {
		return errors.Wrap(err, "could not update object")
	}

	id, err := getIDValue(arg)
	if err != nil {
		return errors.Wrap(err, errmsg)
	}

	values = append([]any{id}, values...)

	commandTag, err := db.Conn.Exec(context.Background(), statement, values...)
	if err != nil {
		return errors.Wrap(err, "could not update object")
	}

	if commandTag.RowsAffected() != 1 {
		return errors.New("incorrect number of rows affected after updating the object")
	}

	return nil
}

func (db *Database) Delete(t reflect.Type, clauses string, args ...any) (int64, error) {
	errmsg := fmt.Sprintf("could not delete objects of type %s", t.Name())

	statement := buildDeleteStatement(t, clauses)
	commandTag, err := db.Conn.Exec(context.Background(), statement, args...)
	if err != nil {
		return 0, errors.Wrap(err, errmsg)
	}

	return commandTag.RowsAffected(), nil
}
