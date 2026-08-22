package money

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/shopspring/decimal"
)

func TestDecimalAdditionHasNoFloatError(t *testing.T) {
	a, err := FromString("0.1")
	if err != nil {
		t.Fatalf("parse 0.1: %v", err)
	}
	b, err := FromString("0.2")
	if err != nil {
		t.Fatalf("parse 0.2: %v", err)
	}

	got := Sum(a, b)
	if !got.Equal(decimal.RequireFromString("0.3")) {
		t.Fatalf("0.1 + 0.2 = %s, want exactly 0.3", got.String())
	}
	if got.String() != "0.3" {
		t.Fatalf("string form = %q, want %q", got.String(), "0.3")
	}
}

func TestAccumulationOfManySmallAmountsIsExact(t *testing.T) {
	cents, err := FromString("0.01")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	values := make([]decimal.Decimal, 1000)
	for i := range values {
		values[i] = cents
	}
	got := Sum(values...)
	if !got.Equal(decimal.RequireFromString("10.00")) {
		t.Fatalf("1000 x 0.01 = %s, want exactly 10.00", got.String())
	}
}

func TestMinorUnitsRoundTrip(t *testing.T) {
	cases := []struct {
		minor int64
		want  string
	}{
		{1999, "19.99"},
		{1, "0.01"},
		{100000, "1000.00"},
		{-250, "-2.50"},
	}
	for _, tc := range cases {
		d := FromMinorUnits(tc.minor)
		if !d.Equal(decimal.RequireFromString(tc.want)) {
			t.Errorf("FromMinorUnits(%d) = %s, want %s", tc.minor, d.String(), tc.want)
		}
		if back := ToMinorUnits(d); back != tc.minor {
			t.Errorf("ToMinorUnits(FromMinorUnits(%d)) = %d, round trip broken", tc.minor, back)
		}
	}
}

func TestToMinorUnitsRoundsHalfAwayFromZero(t *testing.T) {
	d := decimal.RequireFromString("19.995")
	if got := ToMinorUnits(d); got != 2000 {
		t.Fatalf("ToMinorUnits(19.995) = %d, want 2000", got)
	}
	neg := decimal.RequireFromString("-19.995")
	if got := ToMinorUnits(neg); got != -2000 {
		t.Fatalf("ToMinorUnits(-19.995) = %d, want -2000", got)
	}
}

func TestJSONMarshalsAsNumberNotString(t *testing.T) {
	type payload struct {
		Amount Number `json:"amount"`
	}
	p := payload{Amount: Number{decimal.RequireFromString("19.99")}}

	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(raw) != `{"amount":19.99}` {
		t.Fatalf("json = %s, want unquoted number {\"amount\":19.99} (string form breaks the client contract)", raw)
	}

	var back payload
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !back.Amount.Equal(p.Amount.Decimal) {
		t.Fatalf("round trip = %s, want %s", back.Amount.String(), p.Amount.String())
	}
}

func TestNoFloatPathInPublicAPI(t *testing.T) {
	var huge int64 = math.MaxInt64
	d := FromMinorUnits(huge)
	if got := ToMinorUnits(d); got != huge {
		t.Fatalf("int64 boundary round trip = %d, want %d (no float widening allowed)", got, huge)
	}
}
