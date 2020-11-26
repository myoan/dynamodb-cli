package main

import (
	"flag"
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func CreateTable(ddb *dynamodb.DynamoDB, table, pkey, ptype string) {
	fmt.Printf("Create table: %s\n", table)
	params := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(pkey),
				AttributeType: aws.String(ptype),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(pkey),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String(table),
	}

	resp, err := ddb.CreateTable(params)

	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(resp)
}

func ListTable(ddb *dynamodb.DynamoDB) error {
	fmt.Printf("List table\n")
	params := &dynamodb.ListTablesInput{}

	resp, err := ddb.ListTables(params)

	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(resp)
	return nil
}

func DeleteTable(ddb *dynamodb.DynamoDB, table string) {
	fmt.Printf("Delete table: %s\n", table)
	params := &dynamodb.DeleteTableInput{
		TableName: aws.String(table),
	}

	resp, err := ddb.DeleteTable(params)

	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(resp)
}

func ExecStmt(ddb *dynamodb.DynamoDB, filepath string) {
	bytes, err := ioutil.ReadFile(filepath)
	if err != nil {
		panic(err)
	}

	stmt := string(bytes)

	params := &dynamodb.ExecuteStatementInput{
		Statement: aws.String(stmt),
	}

	resp, err := ddb.ExecuteStatement(params)

	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(resp)
}

func main() {
	var (
		c     = flag.String("cmd", "list-table", "command")
		name  = flag.String("table", "", "table name")
		pkey  = flag.String("pkey", "", "primary-key")
		ptype = flag.String("ptype", "", "prymary-type")
		stmt  = flag.String("file", "", "stmt filepath")
	)
	flag.Parse()

	ddb := dynamodb.New(session.New(), aws.NewConfig().WithRegion("ap-northeast-1"))

	switch *c {
	case "create-table":
		CreateTable(ddb, *name, *pkey, *ptype)
	case "list-table":
		ListTable(ddb)
	case "delete-table":
		DeleteTable(ddb, *name)
	case "exec-stmt":
		ExecStmt(ddb, *stmt)
	}
}
