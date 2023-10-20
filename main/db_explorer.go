package main3

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

type DbExplorer struct {
	db *sql.DB
}

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {
	return &DbExplorer{db: db}, nil
}

func (e *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.handleRequest(w, r)
}

func sendJSONError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	errorResponse := map[string]interface{}{"error": message}
	json.NewEncoder(w).Encode(errorResponse)
}

func scanRowToMap(rows *sql.Rows) (map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	columnData := make([]interface{}, len(columns))
	columnPointers := make([]interface{}, len(columns))
	for i, ct := range columnTypes {
		switch ct.DatabaseTypeName() {
		case "INT", "INTEGER", "BIGINT", "TINYINT", "MEDIUMINT", "SMALLINT":
			columnData[i] = new(sql.NullInt64)
		case "FLOAT", "DOUBLE", "DECIMAL":
			columnData[i] = new(sql.NullFloat64)
		default:
			columnData[i] = new(sql.NullString)
		}
		columnPointers[i] = columnData[i]
	}

	if err := rows.Scan(columnPointers...); err != nil {
		return nil, err
	}

	record := make(map[string]interface{})
	for i, colName := range columns {
		switch v := columnData[i].(type) {
		case *sql.NullInt64:
			if v.Valid {
				record[colName] = v.Int64
			} else {
				record[colName] = nil
			}
		case *sql.NullFloat64:
			if v.Valid {
				record[colName] = v.Float64
			} else {
				record[colName] = nil
			}
		case *sql.NullString:
			if v.Valid {
				record[colName] = v.String
			} else {
				record[colName] = nil
			}
		}
	}
	return record, nil
}

func (e *DbExplorer) checkColumnType(columnTypes []*sql.ColumnType, columnName string, value interface{}) bool {
	for _, colType := range columnTypes {
		if colType.Name() == columnName {
			// Get database type name
			dbTypeName := strings.ToUpper(colType.DatabaseTypeName())

			// Check value type
			switch dbTypeName {
			case "INT", "INTEGER", "BIGINT", "TINYINT", "MEDIUMINT", "SMALLINT":
				_, ok := value.(float64)
				return ok
			case "FLOAT", "DOUBLE", "DECIMAL":
				_, ok := value.(float64)
				return ok
			default:
				_, ok := value.(string)
				return ok
			}
		}
	}

	// Column not found
	return false
}

func (e *DbExplorer) getTables(w http.ResponseWriter, r *http.Request) {
	tables := []string{"items", "users"}
	responseData := map[string]interface{}{
		"response": map[string]interface{}{
			"tables": tables,
		},
	}
	response, err := json.Marshal(responseData)
	if err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
}

func (e *DbExplorer) getTableRecords(w http.ResponseWriter, r *http.Request, table string) {
	limit := 5
	offset := 0

	queryParams := r.URL.Query()
	if l, err := strconv.Atoi(queryParams.Get("limit")); err == nil {
		limit = l
	}
	if o, err := strconv.Atoi(queryParams.Get("offset")); err == nil {
		offset = o
	}

	rows, err := e.db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", table, limit, offset))
	if err != nil {
		http.Error(w, "Failed to query records", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	records := make([]map[string]interface{}, 0)
	for rows.Next() {
		record, err := scanRowToMap(rows)
		if err != nil {
			http.Error(w, "Failed to scan row", http.StatusInternalServerError)
			return
		}
		records = append(records, record)
	}

	responseData := map[string]interface{}{
		"response": map[string]interface{}{
			"records": records,
		},
	}

	response, err := json.Marshal(responseData)
	if err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(response)
}

func (e *DbExplorer) getRecord(w http.ResponseWriter, r *http.Request, table string, id string) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE id = %s", table, id)
	rows, err := e.db.Query(query)
	if err != nil {
		http.Error(w, "Failed to get record", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	if !rows.Next() {
		sendJSONError(w, "record not found", http.StatusNotFound)
		return
	}

	record, err := scanRowToMap(rows)
	if err != nil {
		sendJSONError(w, "failed to get record", http.StatusInternalServerError)
		return
	}

	responseData := map[string]interface{}{
		"response": map[string]interface{}{
			"record": record,
		},
	}

	responseJSON, err := json.Marshal(responseData)
	if err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(responseJSON)
}

func (e *DbExplorer) createRecord(w http.ResponseWriter, r *http.Request, table string) {
	decoder := json.NewDecoder(r.Body)
	var record map[string]interface{}
	err := decoder.Decode(&record)
	if err != nil {
		sendJSONError(w, "failed to decode JSON data", http.StatusBadRequest)
		return
	}

	cols := make([]string, 0)
	vals := make([]interface{}, 0)

	for column, value := range record {
		if column == "id" {
			continue
		}
		cols = append(cols, column)
		vals = append(vals, value)
	}

	columns := strings.Join(cols, ", ")
	placeholders := "?" + strings.Repeat(",?", len(cols)-1)

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, columns, placeholders)
	result, err := e.db.Exec(query, vals...)
	if err != nil {
		sendJSONError(w, "failed to create record", http.StatusInternalServerError)
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		sendJSONError(w, "failed to get last inserted ID", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"response": map[string]interface{}{
			"id": id,
		},
	}
	responseJSON, _ := json.Marshal(response)
	w.Header().Set("Content-Type", "application/json")
	w.Write(responseJSON)
}

func (e *DbExplorer) updateRecord(w http.ResponseWriter, r *http.Request, table string, id string) {
	decoder := json.NewDecoder(r.Body)
	var record map[string]interface{}
	err := decoder.Decode(&record)
	if err != nil {
		sendJSONError(w, "failed to decode JSON data", http.StatusBadRequest)
		return
	}

	// Check if id is a number
	_, err = strconv.Atoi(id)
	if err != nil {
		sendJSONError(w, "invalid id", http.StatusBadRequest)
		return
	}

	// Check if id is not being updated
	if record["id"] != nil {
		sendJSONError(w, "id field cannot be updated", http.StatusBadRequest)
		return
	}

	// Get column types for the table
	columnTypes, err := e.db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", table))
	if err != nil {
		sendJSONError(w, "failed to get column types", http.StatusInternalServerError)
		return
	}
	defer columnTypes.Close()
	columnTypesSlice, err := columnTypes.ColumnTypes()
	if err != nil {
		sendJSONError(w, "failed to get column types", http.StatusInternalServerError)
		return
	}

	// Prepare SET clause
	var setClause []string
	for fieldName, fieldValue := range record {
		if fieldName == "id" {
			continue
		}

		if !e.checkColumnType(columnTypesSlice, fieldName, fieldValue) {
			sendJSONError(w, fmt.Sprintf("field %s have invalid type", fieldName), http.StatusBadRequest)
			return
		}

		setClause = append(setClause, fmt.Sprintf("%s = ?", fieldName))
	}

	// Prepare UPDATE query
	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = ?", table, strings.Join(setClause, ", "))
	values := make([]interface{}, 0, len(record))
	for _, v := range record {
		values = append(values, v)
	}
	values = append(values, id)

	// Execute query
	result, err := e.db.Exec(query, values...)
	if err != nil {
		sendJSONError(w, "failed to update record", http.StatusInternalServerError)
		return
	}

	// Check if record was updated
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		sendJSONError(w, "failed to get rows affected", http.StatusInternalServerError)
		return
	}
	if rowsAffected == 0 {
		sendJSONError(w, "record not found", http.StatusNotFound)
		return
	}

	response := map[string]interface{}{
		"response": map[string]interface{}{
			"updated": rowsAffected,
		},
	}
	responseJSON, _ := json.Marshal(response)
	w.Header().Set("Content-Type", "application/json")
	w.Write(responseJSON)
}

func (e *DbExplorer) deleteRecord(w http.ResponseWriter, r *http.Request, table string, id string) {
	defer e.db.Close()

	query := fmt.Sprintf("DELETE FROM %s WHERE id = %s", table, id)
	result, err := e.db.Exec(query)
	if err != nil {
		http.Error(w, "Failed to delete record", http.StatusInternalServerError)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	response := map[string]int64{"deleted": rowsAffected}
	responseJSON, _ := json.Marshal(response)
	w.Header().Set("Content-Type", "application/json")
	w.Write(responseJSON)
}

func (e *DbExplorer) tableExists(tableName string) (bool, error) {
	var exists bool
	err := e.db.QueryRow("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ?)", tableName).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (e *DbExplorer) handleRequest(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	table := ""
	id := ""

	if len(pathParts) > 0 {
		table = pathParts[0]
	}
	if len(pathParts) > 1 {
		id = pathParts[1]
	}

	if table != "" {
		exists, err := e.tableExists(table)
		if err != nil {
			sendJSONError(w, "Failed to check table existence", http.StatusInternalServerError)
			return
		}
		if !exists {
			sendJSONError(w, "unknown table", http.StatusNotFound)
			return
		}
	}

	switch r.Method {
	case http.MethodGet:
		if table == "" {
			e.getTables(w, r)
		} else if id == "" {
			e.getTableRecords(w, r, table)
		} else {
			e.getRecord(w, r, table, id)
		}
	case http.MethodPut:
		if table != "" && id == "" {
			e.createRecord(w, r, table)
		} else {
			http.Error(w, "Invalid request", http.StatusBadRequest)
		}
	case http.MethodPost:
		if table != "" && id != "" {
			e.updateRecord(w, r, table, id)
		} else {
			http.Error(w, "Invalid request", http.StatusBadRequest)
		}
	case http.MethodDelete:
		if table != "" && id != "" {
			e.deleteRecord(w, r, table, id)
		} else {
			http.Error(w, "Invalid request", http.StatusBadRequest)
		}
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
