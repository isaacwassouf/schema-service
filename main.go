package main

import (
	"bytes"
	"context"
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

type SchemaManagementService struct {
	pb.UnimplementedSchemaServiceServer
	schemaManagementServiceDB *db.SchemaManagementServiceDB
}

func (s *SchemaManagementService) CreateTable(ctx context.Context, in *pb.CreateTableRequest) (*pb.CreateTableResponse, error) {
	type Table struct {
		TABLE_NAME string
	}

	templ, err := template.New("create_table").Parse(`CREATE TABLE IF NOT EXISTS {{.TABLE_NAME}} (id SERIAL PRIMARY KEY);`)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to create table")
	}

	var tableSQL bytes.Buffer
	// Execute the template and write the output to a string
	err = templ.Execute(&tableSQL, Table{TABLE_NAME: in.TableName})
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
