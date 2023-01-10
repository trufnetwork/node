package mocks

import (
	"kwil/x/execution"
	"kwil/x/types/databases"
)

var (
	// database
	Db1 = databases.Database{
		Name:  "db1",
		Owner: "0xabc",
		Tables: []*databases.Table{
			&Table1,
			&Table2,
		},
		SQLQueries: []*databases.SQLQuery{
			&Insert1,
			&Insert2,
			&Update1,
			&Update2,
			&Delete1,
			&Delete2,
		},
		Roles: []*databases.Role{
			&Role1,
			&Role2,
		},
		Indexes: []*databases.Index{
			&Index1,
		},
	}

	// tables
	Table1 = databases.Table{
		Name:    "table1",
		Columns: []*databases.Column{&Column1, &Column2},
	}

	Table2 = databases.Table{
		Name:    "table2",
		Columns: []*databases.Column{&Column1, &Column3},
	}

	// columns
	Column1 = databases.Column{
		Name: "col1",
		Type: execution.STRING,
		Attributes: []*databases.Attribute{
			{
				Type:  execution.PRIMARY_KEY,
				Value: nil,
			},
		},
	}

	Column2 = databases.Column{
		Name: "col2",
		Type: execution.INT32,
		Attributes: []*databases.Attribute{
			{
				Type:  execution.MIN,
				Value: 0,
			},
		},
	}

	Column3 = databases.Column{
		Name: "col3",
		Type: execution.BOOLEAN,
	}

	// sql queries

	// insert
	Insert1 = databases.SQLQuery{
		Name:  "insert1",
		Type:  execution.INSERT,
		Table: "table1",
		Params: []*databases.Parameter{
			&Parameter1,
			&Parameter2,
		},
	}

	Insert2 = databases.SQLQuery{
		Name:  "insert2",
		Type:  execution.INSERT,
		Table: "table2",
		Params: []*databases.Parameter{
			&Parameter1,
			&Parameter3,
		},
	}

	// update
	Update1 = databases.SQLQuery{
		Name:  "update1",
		Type:  execution.UPDATE,
		Table: "table1",
		Params: []*databases.Parameter{
			&Parameter1,
			&Parameter2,
		},
		Where: []*databases.WhereClause{
			&WhereClause2,
		},
	}

	Update2 = databases.SQLQuery{
		Name:  "update2",
		Type:  execution.UPDATE,
		Table: "table2",
		Params: []*databases.Parameter{
			&Parameter1,
			&Parameter3,
		},
		Where: []*databases.WhereClause{
			&WhereClause1,
		},
	}

	// delete
	Delete1 = databases.SQLQuery{
		Name:  "delete1",
		Type:  execution.DELETE,
		Table: "table1",
		Where: []*databases.WhereClause{
			&WhereClause2,
		},
	}

	Delete2 = databases.SQLQuery{
		Name:  "delete2",
		Type:  execution.DELETE,
		Table: "table2",
		Where: []*databases.WhereClause{
			&WhereClause1,
		},
	}

	// parameters

	Parameter1 = databases.Parameter{
		Name:     "param1",
		Column:   "col1",
		Static:   true,
		Value:    "",
		Modifier: execution.CALLER,
	}

	Parameter2 = databases.Parameter{
		Name:   "param2",
		Column: "col2",
	}

	Parameter3 = databases.Parameter{
		Name:   "param3",
		Column: "col3",
		Static: false,
	}

	WhereClause1 = databases.WhereClause{
		Name:     "where1",
		Column:   "col3",
		Static:   false,
		Operator: execution.EQUAL,
	}

	WhereClause2 = databases.WhereClause{
		Name:     "where2",
		Column:   "col1",
		Static:   true,
		Operator: execution.EQUAL,
		Value:    "",
		Modifier: execution.CALLER,
	}

	// roles
	Role1 = databases.Role{
		Name:    "role1",
		Default: true,
		Permissions: []string{
			"insert1",
			"update1",
			"delete1",
		},
	}

	Role2 = databases.Role{
		Name: "role2",
		Permissions: []string{
			"insert1",
			"insert2",
			"update1",
			"update2",
			"delete1",
			"delete2",
		},
	}

	// indexes
	Index1 = databases.Index{
		Name:    "my_index",
		Table:   "table1",
		Columns: []string{"col1", "col2"},
		Using:   1,
	}
)