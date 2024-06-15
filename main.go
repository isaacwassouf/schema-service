package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	db "github.com/isaacwassouf/schema-service/database"
	pb "github.com/isaacwassouf/schema-service/protobufs/schema_management_service"
)

type SchemaManagementService struct {
	pb.UnimplementedSchemaServiceServer
	schemaManagementServiceDB *db.SchemaManagementServiceDB
}

func (s *SchemaManagementService) CreateTable(ctx context.Context, in *pb.CreateTableRequest) (*pb.CreateTableResponse, error) {
	return nil, status.Error(codes.NotFound, "not found")
}

func main() {
	// Create a new schemaManagementServiceDB
	schemaManagementServiceDB, err := db.NewSchemaManagementServiceDB()
	if err != nil {
		log.Fatalf("failed to create a new SchemaManagementServiceDB: %v", err)
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
