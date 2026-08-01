// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2026 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

// unifiedExec runs query through QueryResultContext on a raw connection and
// returns either the fully-read resultset (columns + rows, cells copied) or
// the OK-response counters.
type unifiedResponse struct {
	columns      []string
	rows         [][][]byte // nil cell = NULL
	isResultset  bool
	rowsAffected int64
	lastInsertID int64
}

func unifiedExec(ctx context.Context, dbt *DBTest, query string, args []driver.NamedValue) (*unifiedResponse, error) {
	conn, err := dbt.db.Conn(ctx)
	if err != nil {
		dbt.Fatalf("getting conn: %s", err)
	}
	defer conn.Close()

	resp := &unifiedResponse{}
	rawErr := conn.Raw(func(dc any) error {
		mc, ok := dc.(*mysqlConn)
		if !ok {
			dbt.Fatalf("driver conn is %T, not *mysqlConn", dc)
		}
		rows, result, err := mc.QueryResultContext(ctx, query, args)
		if err != nil {
			return err
		}
		if rows == nil {
			resp.rowsAffected, _ = result.RowsAffected()
			resp.lastInsertID, _ = result.LastInsertId()
			return nil
		}
		defer rows.Close()
		resp.isResultset = true
		resp.columns = rows.Columns()
		dest := make([]driver.Value, len(resp.columns))
		for {
			if err := rows.Next(dest); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			row := make([][]byte, len(dest))
			for i, v := range dest {
				if v == nil {
					continue
				}
				b, ok := v.([]byte)
				if !ok {
					dbt.Fatalf("unified cell %d is %T, want []byte (wire pass-through)", i, v)
				}
				row[i] = bytes.Clone(b)
			}
			resp.rows = append(resp.rows, row)
		}
	})
	if rawErr != nil {
		return nil, rawErr
	}
	return resp, nil
}

func TestQueryResultContext(t *testing.T) {
	runTestsParallel(t, dsn, func(dbt *DBTest, tbl string) {
		ctx := context.Background()
		dbt.mustExec("CREATE TABLE " + tbl + " (id INT PRIMARY KEY AUTO_INCREMENT, dt DATETIME, note VARCHAR(16))")

		// OK response: rows must be nil, counters populated.
		resp, err := unifiedExec(ctx, dbt, "INSERT INTO "+tbl+" (dt, note) VALUES ('2026-08-01 12:00:00', 'a'), (NULL, 'b')", nil)
		if err != nil {
			dbt.Fatalf("insert: %s", err)
		}
		if resp.isResultset {
			dbt.Fatal("INSERT returned a resultset")
		}
		if resp.rowsAffected != 2 || resp.lastInsertID != 1 {
			dbt.Fatalf("INSERT counters: affected=%d insertID=%d", resp.rowsAffected, resp.lastInsertID)
		}

		// Resultset: cells arrive as raw wire text, NULL as nil.
		resp, err = unifiedExec(ctx, dbt, "SELECT id, dt, note FROM "+tbl+" ORDER BY id", nil)
		if err != nil {
			dbt.Fatalf("select: %s", err)
		}
		if !resp.isResultset || len(resp.columns) != 3 || len(resp.rows) != 2 {
			dbt.Fatalf("SELECT shape: resultset=%v cols=%v rows=%d", resp.isResultset, resp.columns, len(resp.rows))
		}
		if got := string(resp.rows[0][0]); got != "1" {
			dbt.Errorf("id wire text: %q", got)
		}
		if got := string(resp.rows[0][1]); got != "2026-08-01 12:00:00" {
			dbt.Errorf("datetime wire text: %q", got)
		}
		if resp.rows[1][1] != nil {
			dbt.Errorf("NULL cell: %q", resp.rows[1][1])
		}

		// Empty resultset is still a resultset, not an OK response.
		resp, err = unifiedExec(ctx, dbt, "SELECT id FROM "+tbl+" WHERE 1 = 0", nil)
		if err != nil {
			dbt.Fatalf("empty select: %s", err)
		}
		if !resp.isResultset || len(resp.rows) != 0 {
			dbt.Fatalf("empty SELECT shape: resultset=%v rows=%d", resp.isResultset, len(resp.rows))
		}

		// A statement callers routinely misclassify: CHECK TABLE answers with
		// a resultset even though it reads like an admin/modify statement.
		resp, err = unifiedExec(ctx, dbt, "CHECK TABLE "+tbl, nil)
		if err != nil {
			dbt.Fatalf("check table: %s", err)
		}
		if !resp.isResultset || len(resp.rows) != 1 {
			dbt.Fatalf("CHECK TABLE shape: resultset=%v rows=%d", resp.isResultset, len(resp.rows))
		}
		if got := string(resp.rows[0][3]); got != "OK" {
			dbt.Errorf("CHECK TABLE msg: %q", got)
		}

		// Args require InterpolateParams (pre-write driver.ErrSkip without
		// it), exactly like QueryContext. The harness runs this test under
		// both DSN variants, so accept either outcome — but each must be
		// exact.
		args := []driver.NamedValue{{Ordinal: 1, Value: int64(1)}}
		resp, err = unifiedExec(ctx, dbt, "SELECT note FROM "+tbl+" WHERE id = ?", args)
		if err != nil {
			if !errors.Is(err, driver.ErrSkip) {
				dbt.Fatalf("args select: %s", err)
			}
		} else if len(resp.rows) != 1 || string(resp.rows[0][0]) != "a" {
			dbt.Fatalf("args SELECT rows: %v", resp.rows)
		}

		// Errors surface normally and leave the connection reusable.
		if _, err = unifiedExec(ctx, dbt, "SELECT syntax error from", nil); err == nil {
			dbt.Fatal("syntax error did not surface")
		}
		var myErr *MySQLError
		if _, err = unifiedExec(ctx, dbt, "SELECT * FROM does_not_exist_"+tbl, nil); !errors.As(err, &myErr) {
			dbt.Fatalf("missing table error: %v", err)
		}
		if resp, err = unifiedExec(ctx, dbt, "SELECT COUNT(*) FROM "+tbl, nil); err != nil || string(resp.rows[0][0]) != "2" {
			dbt.Fatalf("conn not reusable after errors: %v %v", resp, err)
		}
	})
}
