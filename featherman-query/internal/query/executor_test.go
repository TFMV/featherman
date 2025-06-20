package query

import "testing"

func TestValidateSQL(t *testing.T) {
	cases := []struct {
		sql string
		ok  bool
	}{
		{"select 1", true},
		{"ATTACH database 'x'", false},
		{"install extension", false},
	}
	for _, c := range cases {
		err := validateSQL(c.sql)
		if c.ok && err != nil {
			t.Fatalf("expected ok for %s: %v", c.sql, err)
		}
		if !c.ok && err == nil {
			t.Fatalf("expected error for %s", c.sql)
		}
	}
}
