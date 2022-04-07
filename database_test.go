package liteorm

import (
	"bytes"
	"flag"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
	"time"
)

type TestItem struct {
	ID           int64  `pgsql:"primary key"`
	StringColumn string `pglen:"25"`
	IntColumn    int
	TimeColumn   time.Time
	BLOBColumn   []byte
}

var TestItemType reflect.Type = reflect.TypeOf((*TestItem)(nil)).Elem()

var host = flag.String("host", "", "database host")
var port = flag.String("port", "", "database port")
var user = flag.String("user", "", "database user")
var password = flag.String("password", "", "database password")
var database = flag.String("database", "", "default database")

var db *Database
var testObject *TestItem

func TestMain(m *testing.M) {
	var err error
	flag.Parse()

	dsnString := fmt.Sprintf("%s=%s ", "host", *host)
	dsnString += fmt.Sprintf("%s=%s ", "port", *port)
	dsnString += fmt.Sprintf("%s=%s ", "user", *user)
	dsnString += fmt.Sprintf("%s=%s ", "password", *password)
	dsnString += fmt.Sprintf("%s=%s ", "database", *database)

	db, err = NewDatabase(dsnString)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	} else {
		os.Exit(m.Run())
	}
}

func TestCreateTable(t *testing.T) {
	err := db.CreateTable(TestItemType, true)
	if err != nil {
		t.Errorf(err.Error())
	}

	err = db.CreateTable(TestItemType, false)
	if err == nil {
		t.Errorf("expected error on second call to CreateTable")
	}
}

func TestTableExists(t *testing.T) {
	exists, err := db.TableExists(TestItemType)
	if err != nil {
		t.Errorf(err.Error())
	}

	if exists != true {
		t.Errorf("table does not exist, but it should")
	}
}

func testEquality(a, b TestItem, t *testing.T) {
	if a.ID != b.ID {
		t.Errorf("mismatch in the ID field of the selected object")
	}

	if a.StringColumn != b.StringColumn {
		t.Errorf("mismatch in the StringColumn field of the selected object")
	}

	if a.IntColumn != b.IntColumn {
		t.Errorf("mismatch in the IntColumn field of the selected object")
	}

	if math.Abs(float64(a.TimeColumn.Sub(b.TimeColumn))) > 1000 /* nanoseconds */ {
		t.Errorf("mismatch in the TimeColumn field of the selected object")
	}

	if !bytes.Equal(a.BLOBColumn, b.BLOBColumn) {
		t.Errorf("mismatch in the BLOBColumn field of the selected object")
	}
}

func TestInsert(t *testing.T) {
	testObject = &TestItem{
		StringColumn: "lashbits.tech",
		IntColumn:    1337,
		TimeColumn:   time.Now().UTC(),
		BLOBColumn:   []byte{0x13, 0x37},
	}

	err := db.Insert(testObject)
	if err != nil {
		t.Errorf("could not insert object - %s", err.Error())
	}

	anotherTestObject := &TestItem{
		StringColumn: "lashbits.tech",
		IntColumn:    1337,
		TimeColumn:   time.Now().UTC(),
		BLOBColumn:   []byte{0x13, 0x37},
	}

	err = db.Insert(anotherTestObject)
	if err != nil {
		t.Errorf("could not insert second object - %s", err.Error())
	}
}

func TestSelectOne(t *testing.T) {
	var selectedTestObject TestItem

	err := db.SelectOne(&selectedTestObject, "where id = $1", testObject.ID)
	if err != nil {
		t.Errorf("could not select object - %s", err.Error())
	}

	testEquality(*testObject, selectedTestObject, t)
}

func TestSelect(t *testing.T) {
	var result []TestItem

	if resultif, err := db.Select(TestItemType, ""); err == nil {
		result = resultif.([]TestItem)
	} else {
		t.Errorf("could not select objects - %s", err.Error())
	}

	if len(result) != 2 {
		t.Errorf("incorrect amount of objects selected - %d instead of 2", len(result))
	}

	if resultif, err := db.Select(TestItemType, "where id = $1", testObject.ID); err == nil {
		result = resultif.([]TestItem)
	} else {
		t.Errorf("could not select objects - %s", err.Error())
	}

	if len(result) != 1 {
		t.Errorf("incorrect amount of objects selected - %d instead of 1", len(result))
	}

	testEquality(*testObject, result[0], t)
}

func TestUpdate(t *testing.T) {
	testObject.StringColumn = "lashbits.tech updated!"
	err := db.UpdateOne(testObject)
	if err != nil {
		t.Errorf("could not update object - %s", err.Error())
	}

	var selectedTestObject TestItem
	err = db.SelectOne(&selectedTestObject, "where id = $1", testObject.ID)
	if err != nil {
		t.Errorf("could not select object - %s", err.Error())
	}

	testEquality(*testObject, selectedTestObject, t)
}

func TestDelete(t *testing.T) {
	rows, err := db.Delete(TestItemType, "where id = $1", testObject.ID+1)
	if err != nil {
		t.Errorf("could not delete object - %s", err.Error())
	}

	if rows != 1 {
		t.Errorf("incorrect amount of objects deleted - %d instead of 1", rows)
	}

	var result []TestItem
	if resultif, err := db.Select(TestItemType, ""); err == nil {
		result = resultif.([]TestItem)
	} else {
		t.Errorf("could not select objects - %s", err.Error())
	}

	if len(result) != 1 {
		t.Errorf("incorrect amount of objects remaining after delete - %d instead of 1", len(result))
	}
}
