package gen

import "fmt"

func genExpr(c *Ctx, depth int) string {
	if depth >= 3 {
		return exprLeaf(c)
	}
	switch c.R.IntN(8) {
	case 0:
		return exprLeaf(c)
	case 1:
		return "(" + genExpr(c, depth+1) + ")"
	case 2:
		return pickString(c.R, []string{"-", "!", "~"}) + genExpr(c, depth+1)
	case 3:
		return genExpr(c, depth+1) + " " + pickString(c.R, []string{"+", "-", "*", "/", "DIV", "MOD", "AND", "OR", "=", "<=>", "LIKE"}) + " " + genExpr(c, depth+1)
	case 4:
		return "CAST(" + genExpr(c, depth+1) + " AS " + pickString(c.R, []string{"CHAR", "SIGNED", "DECIMAL(10,2)", "DATETIME"}) + ")"
	case 5:
		return pickString(c.R, []string{"CONCAT", "COALESCE"}) + "(" + genExpr(c, depth+1) + ", " + genExpr(c, depth+1) + ")"
	case 6:
		return pickString(c.R, []string{"NOW()", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP(6)"})
	default:
		return "CASE WHEN " + genExpr(c, depth+1) + " THEN " + genExpr(c, depth+1) + " ELSE " + genExpr(c, depth+1) + " END"
	}
}

func exprLeaf(c *Ctx) string {
	switch c.R.IntN(8) {
	case 0:
		return fmt.Sprint(c.R.IntN(1000))
	case 1:
		return pickString(c.R, []string{".5", "1e5", "-1.5e-10", "3.14159"})
	case 2:
		return literal(c)
	case 3:
		return pickString(c.R, []string{"X'CAFE'", "b'1010'", "0xCAFE", "0b1010"})
	case 4:
		return pickString(c.R, []string{"NULL", "TRUE", "FALSE"})
	case 5:
		return quoteMaybe(c, pickString(c.R, c.V.Columns))
	case 6:
		return "_utf8mb4'comma,value */ FOR'"
	default:
		return "N'doubled '' quote'"
	}
}

func literal(c *Ctx) string {
	return pickString(c.R, []string{
		"'x'",
		"'comma,value'",
		"'quote''s'",
		"'has */ marker'",
		"'FOR token'",
		`"double string"`,
		"_utf8mb4'intro,*/'",
		"X'CAFE'",
		"0xCAFE",
		"N'national'",
	})
}
