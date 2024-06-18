package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"text/template"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	db "github.com/isaacwassouf/schema-service/database"
	pb "github.com/isaacwassouf/schema-service/protobufs/schema_management_service"
	utils "github.com/isaacwassouf/schema-service/utils"
)

type Column struct {
	Name         string
	Type         string
	NotNullable  bool
	IsUnique     bool
	IsPrimaryKey bool
}

type Table struct {
	Name    string
	Columns []Column
}

type SchemaManagementService struct {
	pb.UnimplementedSchemaServiceServer
	schemaManagementServiceDB *db.SchemaManagementServiceDB
}

func (s *SchemaManagementService) CreateTable(ctx context.Context, in *pb.CreateTableRequest) (*pb.CreateTableResponse, error) {
	// Check if the table exists
	tableExists, err := utils.CheckTableExists(s.schemaManagementServiceDB.Db, in.TableName)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to check if table exists")
	}
	if tableExists {
		return nil, status.Error(codes.AlreadyExists, "table already exists")
	}

	// read the file
	templateFile, err := utils.ReadTemplateFile("templates/create_table.tmpl")
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to read template file")
	}

	// create the template from the file
	createTableTemplate, err := template.New("create_table").Parse(templateFile)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to create table")
	}

	// create the columns slice
	columns := make([]Column, len(in.Columns))
	for i, column := range in.Columns {
		var columnType string
		// map the column type to the SQL type
		switch column.Type.(type) {
		case *pb.Column_IntColumn:
			columnType, err = utils.GetIntColumnType(column)
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, "invalid integer column type")
			}
		case *pb.Column_BoolColumn:
			columnType = "BOOLEAN"
		case *pb.Column_TimestampColumn:
			columnType = "TIMESTAMP"
		case *pb.Column_VarcharColumn:
			columnType, err = utils.GetVarCharColumnType(column)
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, "invalid varchar column type")
			}
		case nil:
			return nil, status.Error(codes.InvalidArgument, "column type is required")
		default:
			return nil, status.Error(codes.InvalidArgument, "invalid column type")
		}

		columns[i] = Column{
			Name:         column.Name,
			Type:         columnType,
			NotNullable:  column.NotNullable,
			IsUnique:     column.IsUnique,
			IsPrimaryKey: column.IsPrimaryKey,
		}
	}

	var tableSQL bytes.Buffer
	// Execute the template and write the output to a string
	err = createTableTemplate.Execute(&tableSQL, Table{
		Name:    in.TableName,
		Columns: columns,
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to execute template")
	}

	// Create the table
	_, err = s.schemaManagementServiceDB.Db.Exec(tableSQL.String())
	if err != nil {
		log.Printf("failed to create table: %v", err)
		return nil, status.Error(codes.Internal, "failed to create table")
	}

	return &pb.CreateTableResponse{Message: tableSQL.String()}, nil
}

func (s *SchemaManagementService) DropTable(ctx context.Context, in *pb.DropTableRequest) (*pb.DropTableResponse, error) {
	// Check if the table exists
	tableExists, err := utils.CheckTableExists(s.schemaManagementServiceDB.Db, in.TableName)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to check if table exists")
	}
	if !tableExists {
		return nil, status.Error(codes.NotFound, "table not found")
	}

	// Drop the table
	_, err = s.schemaManagementServiceDB.Db.Exec(fmt.Sprintf("DROP TABLE %s", in.TableName))
	if err != nil {
		log.Printf("failed to drop table: %v", err)
		return nil, status.Error(codes.Internal, "failed to drop table")
	}

	return &pb.DropTableResponse{Message: "table dropped"}, nil
}

func main() {
	// load the environment variables from the .env file
	err := utils.LoadEnvVarsFromFile()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	// Create a new schemaManagementServiceDB
	schemaManagementServiceDB, err := db.NewSchemaManagementServiceDB()
	if err != nil {
		log.Fatalf("failed to create a new SchemaManagementServiceDB: %v", err)
	}
	// ping the database
	err = schemaManagementServiceDB.Db.Ping()
	if err != nil {
		log.Fatalf("failed to ping the database: %v", err)
	}

	// Start the server
	ls, err := net.Listen("tcp", ":8084")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterSchemaServiceServer(s, &SchemaManagementService{
		schemaManagementServiceDB: schemaManagementServiceDB,
	})

	log.Printf("Server listening at %v", ls.Addr())

	if err := s.Serve(ls); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
