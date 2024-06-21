package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/isaacwassouf/schema-service/shared"
	"log"
	"net"
	"text/template"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

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
	DefaultValue string
}

type Table struct {
	TableName   string
	Columns     []Column
	ForeignKeys []shared.ForeignKey
}

type AddColumnPayload struct {
	TableName string
	Column    Column
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
			DefaultValue: column.DefaultValue,
		}
	}

	foreignKeys := make([]shared.ForeignKey, len(in.ForeignKeys))
	for i, fk := range in.ForeignKeys {
		foreignKeys[i] = shared.ForeignKey{
			ColumnName:          fk.ColumnName,
			ReferenceTableName:  fk.ReferenceTableName,
			ReferenceColumnName: fk.ReferenceColumnName,
		}
		// map the enums to the string values
		utils.MapReferentialActionsEnumToString(fk, &foreignKeys[i])

		// Check if the reference table exists
		referenceTableExists, err := utils.CheckTableExists(s.schemaManagementServiceDB.Db, fk.ReferenceTableName)
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to check if reference table exists")
		}
		if !referenceTableExists {
			return nil, status.Error(codes.NotFound, "reference table not found")
		}

		// Check if the reference column exists
		referenceColumnExists, err := utils.CheckColumnExists(s.schemaManagementServiceDB.Db, fk.ReferenceTableName, fk.ReferenceColumnName)
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to check if reference column exists")
		}

		if !referenceColumnExists {
			return nil, status.Error(codes.NotFound, "reference column not found")
		}
	}

	var tableSQL bytes.Buffer
	// Execute the template and write the output to a string
	err = createTableTemplate.Execute(&tableSQL, Table{
		TableName:   in.TableName,
		Columns:     columns,
		ForeignKeys: foreignKeys,
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

func (s *SchemaManagementService) DropColumn(ctx context.Context, in *pb.DropColumnRequest) (*pb.DropColumnResponse, error) {
	// Check if the table exists
	tableExists, err := utils.CheckTableExists(s.schemaManagementServiceDB.Db, in.TableName)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to check if table exists")
	}
	if !tableExists {
		return nil, status.Error(codes.NotFound, "table not found")
	}

	// Check if the column exists
	columnExists, err := utils.CheckColumnExists(s.schemaManagementServiceDB.Db, in.TableName, in.ColumnName)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to check if column exists")
	}
	if !columnExists {
		return nil, status.Error(codes.NotFound, "column not found")
	}

	// read the file
	templateFile, err := utils.ReadTemplateFile("templates/drop_column.tmpl")
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to read template file")
	}

	// create the template from the file
	dropColumnTemplate, err := template.New("create_table").Parse(templateFile)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to drop column")
	}

	var dropColumnSQL bytes.Buffer
	// Execute the template and write the output to a string
	err = dropColumnTemplate.Execute(&dropColumnSQL, struct {
		TableName  string
		ColumnName string
	}{
		TableName:  in.TableName,
		ColumnName: in.ColumnName,
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to execute template")
	}

	// Drop the column
	_, err = s.schemaManagementServiceDB.Db.Exec(dropColumnSQL.String())
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to drop column")
	}

	return &pb.DropColumnResponse{Message: "column dropped"}, nil
}

func (s *SchemaManagementService) AddColumn(ctx context.Context, in *pb.AddColumnRequest) (*pb.AddColumnResponse, error) {
	// Check if the table exists
	tableExists, err := utils.CheckTableExists(s.schemaManagementServiceDB.Db, in.TableName)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to check if table exists")
	}
	if !tableExists {
		return nil, status.Error(codes.NotFound, "table not found")
	}

	// Check if the column exists
	columnExists, err := utils.CheckColumnExists(s.schemaManagementServiceDB.Db, in.TableName, in.Column.Name)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to check if column exists")
	}
	if columnExists {
		return nil, status.Error(codes.AlreadyExists, "column already exists")
	}

	// read the file
	templateFile, err := utils.ReadTemplateFile("templates/add_column.tmpl")
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to read template file")
	}

	// create the template from the file
	addColumnTemplate, err := template.New("create_table").Parse(templateFile)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to add column")
	}

	var columnType string
	// map the column type to the SQL type
	switch in.Column.Type.(type) {
	case *pb.Column_IntColumn:
		columnType, err = utils.GetIntColumnType(in.Column)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid integer column type")
		}
	case *pb.Column_BoolColumn:
		columnType = "BOOLEAN"
	case *pb.Column_TimestampColumn:
		columnType = "TIMESTAMP"
	case *pb.Column_VarcharColumn:
		columnType, err = utils.GetVarCharColumnType(in.Column)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid varchar column type")
		}
	case nil:
		return nil, status.Error(codes.InvalidArgument, "column type is required")
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid column type")
	}

	// read the file
	var addColumnSQL bytes.Buffer
	// Execute the template and write the output to a string
	err = addColumnTemplate.Execute(&addColumnSQL, AddColumnPayload{
		TableName: in.TableName,
		Column: Column{
			Name:         in.Column.Name,
			Type:         columnType,
			NotNullable:  in.Column.NotNullable,
			IsUnique:     in.Column.IsUnique,
			IsPrimaryKey: in.Column.IsPrimaryKey,
		},
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to execute template")
	}

	// Add the column
	_, err = s.schemaManagementServiceDB.Db.Exec(addColumnSQL.String())
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to add column")
	}

	return &pb.AddColumnResponse{Message: "column added"}, nil
}

func (s *SchemaManagementService) ListTables(ctx context.Context, in *emptypb.Empty) (*pb.ListTablesResponse, error) {
	// read the file
	templateFile, err := utils.ReadTemplateFile("templates/list_tables.tmpl")
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to read template file")
	}

	// create the template from the file
	listTablesTemplate, err := template.New("list_tables").Parse(templateFile)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to list tables")
	}

	// get the database name from the env vars
	dbName := utils.GetEnvVar("MYSQL_DATABASE", "database")

	// Execute the template and write the output to a string
	var listTablesSQL bytes.Buffer
	err = listTablesTemplate.Execute(&listTablesSQL, struct {
		DatabaseName string
	}{
		DatabaseName: dbName,
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to execute template")
	}

	// Get the list of tables
	rows, err := s.schemaManagementServiceDB.Db.Query(listTablesSQL.String())
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to list tables")
	}
	defer rows.Close()
	//
	var tables []*pb.TableDetails
	for rows.Next() {
		// var tableDetails pb.TableDetails
		var tableName string
		var rowsCount uint64
		var tableSize uint64
		err := rows.Scan(&tableName, &rowsCount, &tableSize)
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to scan table details")
		}
		tables = append(tables, &pb.TableDetails{
			TableName: tableName,
			RowsCount: rowsCount,
			TableSize: tableSize,
		})
	}

	return &pb.ListTablesResponse{Tables: tables}, nil
}

func (s *SchemaManagementService) ListColumns(ctx context.Context, in *pb.ListColumnsRequest) (*pb.ListColumnsResponse, error) {
	tableExists, err := utils.CheckTableExists(s.schemaManagementServiceDB.Db, in.TableName)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to check if table exists")
	}
	if !tableExists {
		return nil, status.Error(codes.NotFound, "table not found")
	}

	// read the file
	templateFile, err := utils.ReadTemplateFile("templates/list_columns.tmpl")
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to read template file")
	}

	// create the template from the file
	listColumnsTemplate, err := template.New("list_columns").Parse(templateFile)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to list columns")
	}

	// get the database name from the env vars
	dbName := utils.GetEnvVar("MYSQL_DATABASE", "database")

	// Execute the template and write the output to a string
	var listColumnsSQL bytes.Buffer
	err = listColumnsTemplate.Execute(&listColumnsSQL, struct {
		DatabaseName string
	}{
		DatabaseName: dbName,
	})
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to execute template")
	}

	// execute the query and replace the ? with the table name
	rows, err := s.schemaManagementServiceDB.Db.Query(listColumnsSQL.String(), in.TableName)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to list columns")
	}
	defer rows.Close()

	var columns []*pb.Column
	var foreignKeys []*pb.ForeignKey
	for rows.Next() {
		var rawColumnDetails shared.RawColumnDetails
		err := rows.Scan(
			&rawColumnDetails.ColumnName,
			&rawColumnDetails.DataType,
			&rawColumnDetails.ColumnType,
			&rawColumnDetails.IsNullable,
			&rawColumnDetails.ColumnDefault,
			&rawColumnDetails.MaxLength,
			&rawColumnDetails.Extra,
			&rawColumnDetails.IsUnique,
			&rawColumnDetails.IsPrimary,
			&rawColumnDetails.IsForeign,
			&rawColumnDetails.ForeignKey.ReferenceTableName,
			&rawColumnDetails.ForeignKey.ReferenceColumnName,
			&rawColumnDetails.ForeignKey.OnUpdate,
			&rawColumnDetails.ForeignKey.OnDelete,
		)
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to scan column details")
		}

		column, err := utils.GetColumnFromType(&rawColumnDetails)
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to get column from type")
		}

		// set the name of the column
		column.Name = rawColumnDetails.ColumnName

		// check if the column is unique
		if rawColumnDetails.IsUnique {
			column.IsUnique = true
		}

		// check if the column is a primary key
		if rawColumnDetails.IsPrimary {
			column.IsPrimaryKey = true
		}

		// check if the column is nullable
		if rawColumnDetails.IsNullable == "NO" {
			column.NotNullable = true
		}

		// check if there is a default value
		if rawColumnDetails.ColumnDefault.Valid {
			column.DefaultValue = rawColumnDetails.ColumnDefault.String
		}

		if rawColumnDetails.IsForeign {
			foreignKey := &pb.ForeignKey{
				ColumnName:          rawColumnDetails.ColumnName,
				ReferenceTableName:  rawColumnDetails.ForeignKey.ReferenceTableName.String,
				ReferenceColumnName: rawColumnDetails.ForeignKey.ReferenceColumnName.String,
			}

			// map the referential actions string to the enum
			utils.MapReferentialActionsStringToEnum(&shared.ForeignKey{
				OnUpdate: rawColumnDetails.ForeignKey.OnUpdate.String,
				OnDelete: rawColumnDetails.ForeignKey.OnDelete.String,
			}, foreignKey)

			foreignKeys = append(foreignKeys, foreignKey)
		}

		// add the column to the columns slice
		columns = append(columns, column)
	}

	return &pb.ListColumnsResponse{Columns: columns, ForeignKeys: foreignKeys}, nil
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
