package money

import (
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
)

type Number struct {
	decimal.Decimal
}

func (n Number) MarshalJSON() ([]byte, error) {
	return []byte(n.String()), nil
}

func (n *Number) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		return fmt.Errorf("money: empty JSON number")
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return fmt.Errorf("money: parse %q: %w", s, err)
	}
	n.Decimal = d
	return nil
}

func FromString(s string) (decimal.Decimal, error) {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("money: parse %q: %w", s, err)
	}
	return d, nil
}

func Sum(values ...decimal.Decimal) decimal.Decimal {
	if len(values) == 0 {
		return decimal.Zero
	}
	return decimal.Sum(values[0], values[1:]...)
}

func FromMinorUnits(minor int64) decimal.Decimal {
	return decimal.New(minor, -2)
}

func ToMinorUnits(d decimal.Decimal) int64 {
	return d.Shift(2).Round(0).IntPart()
}
