package date

import (
	"github.com/golang-sql/civil"
	"time"
)

func MustParseDate(date string) civil.Date {
	parsed, err := time.Parse("2006-01-02", date)
	if err != nil {
		panic(err)
	}
	return civil.DateOf(parsed)
}
