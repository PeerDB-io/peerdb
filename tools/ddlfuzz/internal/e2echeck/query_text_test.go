package e2echeck

import "testing"

func TestEquivalentQueryText(t *testing.T) {
	tests := []struct {
		name      string
		submitted string
		binlog    string
		maria     bool
		want      bool
	}{
		{
			name:      "boundary whitespace",
			submitted: " \tALTER TABLE t ADD COLUMN n1 INT \r\n",
			binlog:    "ALTER TABLE t ADD COLUMN n1 INT",
			want:      true,
		},
		{
			name:      "final statement terminator",
			submitted: "ALTER TABLE t ADD COLUMN n1 INT;",
			binlog:    "ALTER TABLE t ADD COLUMN n1 INT",
			want:      true,
		},
		{
			name:      "trailing empty statements",
			submitted: "ALTER TABLE t ADD COLUMN n1 INT; ;",
			binlog:    "ALTER TABLE t ADD COLUMN n1 INT",
			want:      true,
		},
		{
			name:      "trailing empty statements after quoted semicolon",
			submitted: "ALTER TABLE t ADD COLUMN n1 INT COMMENT ';'; ;",
			binlog:    "ALTER TABLE t ADD COLUMN n1 INT COMMENT ';'",
			want:      true,
		},
		{
			name:      "nonempty second statement is not normalized",
			submitted: "ALTER TABLE t ADD COLUMN n1 INT; ALTER TABLE t ADD COLUMN n2 INT",
			binlog:    "ALTER TABLE t ADD COLUMN n1 INT",
			want:      false,
		},
		{
			name:      "mariadb reversed skipped comment plainified",
			submitted: "ALTER TABLE fixture ADD n1 INT /*!!11050 NOT NULL*/",
			binlog:    "ALTER TABLE fixture ADD n1 INT /*  11050 NOT NULL*/",
			maria:     true,
			want:      true,
		},
		{
			name:      "mariadb mysql skipped comment plainified",
			submitted: "ALTER TABLE fixture ADD n1 INT /*!80000NOT NULL*/",
			binlog:    "ALTER TABLE fixture ADD n1 INT /* 80000NOT NULL*/",
			maria:     true,
			want:      true,
		},
		{
			name:      "mysql reversed comment is not normalized",
			submitted: "ALTER TABLE fixture ADD n1 INT /*!!11050 NOT NULL*/",
			binlog:    "ALTER TABLE fixture ADD n1 INT /*  11050 NOT NULL*/",
			want:      false,
		},
		{
			name:      "mysql version comment is not normalized",
			submitted: "ALTER TABLE fixture ADD n1 INT /*!80000NOT NULL*/",
			binlog:    "ALTER TABLE fixture ADD n1 INT /* 80000NOT NULL*/",
			want:      false,
		},
		{
			name:      "quoted comment text is not normalized",
			submitted: "ALTER TABLE t ADD COLUMN c VARCHAR(64) DEFAULT '/*!!11050 x*/'",
			binlog:    "ALTER TABLE t ADD COLUMN c VARCHAR(64) DEFAULT '/*  11050 x*/'",
			maria:     true,
			want:      false,
		},
		{
			name:      "different query",
			submitted: "ALTER TABLE t ADD COLUMN n1 INT",
			binlog:    "ALTER TABLE t ADD COLUMN n1 BIGINT",
			maria:     true,
			want:      false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := EquivalentQueryText(tc.submitted, tc.binlog, tc.maria); got != tc.want {
				t.Fatalf("EquivalentQueryText() = %v, want %v", got, tc.want)
			}
		})
	}
}
