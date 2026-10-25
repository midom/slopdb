package sql

import (
	"fmt"
	"hash/crc32"
	"math"
	"math/big"
	"math/bits"
	"strconv"
	"strings"
)

func mysqlNumericFunctions() map[string]mysqlFunctionSpec {
	functions := make(map[string]mysqlFunctionSpec)
	unary := func(name string, operation func(float64) float64) {
		functions[name] = mysqlFunctionSpec{Min: 1, Max: 1, Return: ColReal, Eval: func(s *Schema, args []Value) (Value, error) {
			if args[0].Null {
				return NewNullValue(), nil
			}
			return mysqlMathResult(s, name, operation(toFloat(mysqlFunctionNumber(args[0]))))
		}}
	}
	for name, operation := range map[string]func(float64) float64{
		"SQRT": math.Sqrt, "EXP": math.Exp, "LN": math.Log, "LOG2": math.Log2,
		"LOG10": math.Log10, "SIN": math.Sin, "COS": math.Cos, "TAN": math.Tan,
		"ASIN": math.Asin, "ACOS": math.Acos, "DEGREES": func(v float64) float64 { return v * (180 / math.Pi) },
		"RADIANS": func(v float64) float64 { return v * (math.Pi / 180) },
		"COT":     func(v float64) float64 { return 1 / math.Tan(v) },
	} {
		unary(name, operation)
	}
	functions["PI"] = mysqlFunctionSpec{Min: 0, Max: 0, Return: ColReal, Eval: func(_ *Schema, _ []Value) (Value, error) { return NewRealValue(math.Pi), nil }}
	functions["LOG"] = mysqlFunctionSpec{Min: 1, Max: 2, Return: ColReal, Eval: func(s *Schema, args []Value) (Value, error) {
		if mysqlAnyNull(args) {
			return NewNullValue(), nil
		}
		x := toFloat(mysqlFunctionNumber(args[0]))
		if len(args) == 1 {
			return mysqlMathResult(s, "LOG", math.Log(x))
		}
		y := toFloat(mysqlFunctionNumber(args[1]))
		if x <= 0 || x == 1 || y <= 0 {
			mysqlNumericWarning(s, 3020, "Invalid argument for logarithm")
			return NewNullValue(), nil
		}
		return mysqlMathResult(s, "LOG", math.Log(y)/math.Log(x))
	}}
	functions["ATAN"] = mysqlFunctionSpec{Min: 1, Max: 2, Return: ColReal, Eval: func(s *Schema, args []Value) (Value, error) {
		if mysqlAnyNull(args) {
			return NewNullValue(), nil
		}
		x := toFloat(mysqlFunctionNumber(args[0]))
		if len(args) == 1 {
			return mysqlMathResult(s, "ATAN", math.Atan(x))
		}
		return mysqlMathResult(s, "ATAN", math.Atan2(x, toFloat(mysqlFunctionNumber(args[1]))))
	}}
	functions["ATAN2"] = functions["ATAN"]
	spec := functions["ATAN2"]
	spec.Min = 2
	functions["ATAN2"] = spec
	functions["POW"] = mysqlFunctionSpec{Min: 2, Max: 2, Return: ColReal, Eval: func(s *Schema, args []Value) (Value, error) {
		if mysqlAnyNull(args) {
			return NewNullValue(), nil
		}
		return mysqlMathResult(s, "POW", math.Pow(toFloat(mysqlFunctionNumber(args[0])), toFloat(mysqlFunctionNumber(args[1]))))
	}}
	functions["POWER"] = functions["POW"]
	functions["SIGN"] = mysqlFunctionSpec{Min: 1, Max: 1, Return: ColInteger, Eval: func(_ *Schema, args []Value) (Value, error) {
		if args[0].Null {
			return NewNullValue(), nil
		}
		n, _, err := valueDecimal(mysqlFunctionNumber(args[0]))
		if err != nil {
			return NewNullValue(), err
		}
		return NewIntValue(int64(n.Sign())), nil
	}}
	for _, name := range []string{"CEIL", "CEILING", "FLOOR"} {
		name := name
		functions[name] = mysqlFunctionSpec{Min: 1, Max: 1, Metadata: mysqlIntegralFunctionMetadata, Eval: func(_ *Schema, args []Value) (Value, error) {
			return mysqlIntegralFunction(args[0], name != "FLOOR")
		}}
	}
	functions["TRUNCATE"] = mysqlFunctionSpec{Min: 2, Max: 2, Metadata: mysqlNumericFunctionMetadata, Eval: func(_ *Schema, args []Value) (Value, error) {
		if mysqlAnyNull(args) {
			return NewNullValue(), nil
		}
		return mysqlTruncateNumber(args[0], mysqlFunctionInt(args[1]))
	}}
	functions["MOD"] = mysqlFunctionSpec{Min: 2, Max: 2, Metadata: mysqlModMetadata, Eval: mysqlModuloFunction}
	functions["CONV"] = mysqlFunctionSpec{Min: 3, Max: 3, Return: ColText, Eval: func(_ *Schema, args []Value) (Value, error) {
		if mysqlAnyNull(args) {
			return NewNullValue(), nil
		}
		return mysqlConvertBase(args[0], mysqlFunctionInt(args[1]), mysqlFunctionInt(args[2])), nil
	}}
	for _, name := range []string{"BIN", "OCT"} {
		base := 2
		if name == "OCT" {
			base = 8
		}
		functions[name] = mysqlFunctionSpec{Min: 1, Max: 1, Return: ColText, Eval: func(_ *Schema, args []Value) (Value, error) {
			if args[0].Null {
				return NewNullValue(), nil
			}
			return NewTextValue(strconv.FormatUint(mysqlNumericUint(args[0]), base)), nil
		}}
	}
	functions["CRC32"] = mysqlFunctionSpec{Min: 1, Max: 1, Return: ColUnsigned, Eval: func(_ *Schema, args []Value) (Value, error) {
		if args[0].Null {
			return NewNullValue(), nil
		}
		return NewUnsignedValue(uint64(crc32.ChecksumIEEE([]byte(mysqlStringValue(args[0]))))), nil
	}}
	functions["BIT_COUNT"] = mysqlFunctionSpec{Min: 1, Max: 1, Return: ColInteger, Eval: func(_ *Schema, args []Value) (Value, error) {
		v := args[0]
		if v.Null {
			return NewNullValue(), nil
		}
		count := 0
		if v.Type == ColBlob {
			for _, b := range v.Blob {
				count += bits.OnesCount8(b)
			}
		} else {
			count = bits.OnesCount64(mysqlNumericUint(v))
		}
		return NewIntValue(int64(count)), nil
	}}
	functions["FORMAT"] = mysqlFunctionSpec{Min: 2, Max: 3, Return: ColText, Eval: mysqlFormatFunction}
	return functions
}

func mysqlNumericWarning(s *Schema, code uint16, message string) {
	if options, ok := MySQLSession(s.ctx); ok && options.Warning != nil {
		options.Warning(code, message)
	}
}

func mysqlMathResult(s *Schema, name string, result float64) (Value, error) {
	if math.IsNaN(result) || math.IsInf(result, -1) {
		mysqlNumericWarning(s, 3020, "Invalid argument for "+name)
		return NewNullValue(), nil
	}
	if math.IsInf(result, 1) {
		return NewNullValue(), fmt.Errorf("DOUBLE value is out of range in %s", name)
	}
	return NewRealValue(result), nil
}

func mysqlNumericFunctionMetadata(args []ColumnDef) ColumnDef {
	if len(args) == 0 {
		return ColumnDef{ColType: ColReal}
	}
	c := args[0]
	if c.ColType != ColInteger && c.ColType != ColUnsigned && c.ColType != ColDecimal {
		return ColumnDef{ColType: ColReal}
	}
	return c
}

func mysqlIntegralFunctionMetadata(args []ColumnDef) ColumnDef {
	c := mysqlNumericFunctionMetadata(args)
	if c.ColType == ColDecimal {
		c.Scale = 0
		c.Precision = min(65, max(1, c.Precision)+1)
		c.Declared = "DECIMAL"
	}
	return c
}

func mysqlModMetadata(args []ColumnDef) ColumnDef {
	if len(args) < 2 {
		return ColumnDef{ColType: ColReal}
	}
	if args[0].ColType == ColReal || args[1].ColType == ColReal || args[0].ColType == ColText || args[1].ColType == ColText || args[0].ColType == ColBlob || args[1].ColType == ColBlob {
		return ColumnDef{ColType: ColReal}
	}
	if args[0].ColType == ColDecimal || args[1].ColType == ColDecimal {
		return ColumnDef{ColType: ColDecimal, Declared: "DECIMAL", Precision: 65, Scale: max(args[0].Scale, args[1].Scale)}
	}
	if args[0].ColType == ColUnsigned {
		return ColumnDef{ColType: ColUnsigned, Unsigned: true}
	}
	return ColumnDef{ColType: ColInteger}
}

func mysqlIntegralFunction(v Value, ceil bool) (Value, error) {
	if v.Null {
		return NewNullValue(), nil
	}
	if v.Type == ColInteger || v.Type == ColUnsigned {
		return v, nil
	}
	if v.Type != ColDecimal {
		x := toFloat(mysqlFunctionNumber(v))
		if ceil {
			x = math.Ceil(x)
		} else {
			x = math.Floor(x)
		}
		return NewRealValue(x), nil
	}
	n, scale, err := valueDecimal(v)
	if err != nil {
		return NewNullValue(), err
	}
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(n, decimalPower(scale), r)
	if r.Sign() != 0 {
		if ceil && n.Sign() > 0 {
			q.Add(q, big.NewInt(1))
		}
		if !ceil && n.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		}
	}
	return Value{Type: ColDecimal, Text: q.String()}, nil
}

func mysqlTruncateNumber(v Value, places int64) (Value, error) {
	if v.Type == ColReal || !isNumeric(v) {
		x := toFloat(mysqlFunctionNumber(v))
		if places > 308 {
			return NewRealValue(x), nil
		}
		if places < -308 {
			return NewRealValue(math.Copysign(0, x)), nil
		}
		factor := math.Pow10(int(places))
		scaled := x * factor
		if math.IsInf(scaled, 0) {
			return NewRealValue(x), nil
		}
		return NewRealValue(math.Trunc(scaled) / factor), nil
	}
	n, scale, err := valueDecimal(v)
	if err != nil {
		return NewNullValue(), err
	}
	if places >= int64(scale) {
		return v, nil
	}
	if places < -maxDecimalDigits {
		n.SetInt64(0)
		places = 0
	} else {
		factor := decimalPower(scale - int(places))
		n.Quo(n, factor)
		if places < 0 {
			n.Mul(n, decimalPower(int(-places)))
			places = 0
		}
	}
	switch v.Type {
	case ColInteger:
		return NewIntValue(n.Int64()), nil
	case ColUnsigned:
		return NewUnsignedValue(n.Uint64()), nil
	}
	return Value{Type: ColDecimal, Text: decimalText(n, int(places))}, nil
}

func mysqlModuloFunction(s *Schema, args []Value) (Value, error) {
	if mysqlAnyNull(args) {
		return NewNullValue(), nil
	}
	left, right := mysqlFunctionNumber(args[0]), mysqlFunctionNumber(args[1])
	if toFloat(right) == 0 {
		mysqlNumericWarning(s, 1365, "Division by 0")
		return NewNullValue(), nil
	}
	if args[0].Type == ColReal || args[1].Type == ColReal || !isNumeric(args[0]) || !isNumeric(args[1]) {
		return NewRealValue(math.Mod(toFloat(left), toFloat(right))), nil
	}
	a, as, err := valueDecimal(left)
	if err != nil {
		return NewNullValue(), err
	}
	b, bs, err := valueDecimal(right)
	if err != nil {
		return NewNullValue(), err
	}
	scale := max(as, bs)
	a.Mul(a, decimalPower(scale-as))
	b.Mul(b, decimalPower(scale-bs))
	a.Rem(a, b)
	if args[0].Type == ColDecimal || args[1].Type == ColDecimal {
		return Value{Type: ColDecimal, Text: decimalText(a, scale)}, nil
	}
	if args[0].Type == ColUnsigned {
		return NewUnsignedValue(a.Uint64()), nil
	}
	return NewIntValue(a.Int64()), nil
}

func mysqlNumericUint(v Value) uint64 {
	if v.Type == ColUnsigned {
		return v.Uint
	}
	if v.Type == ColInteger {
		return uint64(v.Int)
	}
	n, scale, err := valueDecimal(mysqlFunctionNumber(v))
	if err != nil {
		return 0
	}
	n.Quo(n, decimalPower(scale))
	modulus := new(big.Int).Lsh(big.NewInt(1), 64)
	n.Mod(n, modulus)
	return n.Uint64()
}

func mysqlConvertBase(value Value, from, to int64) Value {
	if from == math.MinInt64 || to == math.MinInt64 {
		return NewNullValue()
	}
	f, t := from, to
	if f < 0 {
		f = -f
	}
	if t < 0 {
		t = -t
	}
	if f < 2 || f > 36 || t < 2 || t > 36 {
		return NewNullValue()
	}
	text := strings.TrimSpace(mysqlStringValue(value))
	negative := false
	if len(text) > 0 && (text[0] == '+' || text[0] == '-') {
		negative = text[0] == '-'
		text = text[1:]
	}
	limit := uint64(math.MaxUint64)
	if from < 0 {
		limit = math.MaxInt64
		if negative {
			limit++
		}
	}
	var number uint64
	overflow := false
	for _, char := range []byte(text) {
		digit := byte(255)
		if char >= '0' && char <= '9' {
			digit = char - '0'
		}
		if char >= 'a' && char <= 'z' {
			digit = char - 'a' + 10
		}
		if char >= 'A' && char <= 'Z' {
			digit = char - 'A' + 10
		}
		if uint64(digit) >= uint64(f) {
			break
		}
		if number > (limit-uint64(digit))/uint64(f) {
			overflow = true
			number = limit
		} else if !overflow {
			number = number*uint64(f) + uint64(digit)
		}
	}
	if negative {
		if overflow && from > 0 {
			number = 0
		} else {
			number = 0 - number
		}
	}
	if to < 0 {
		return NewTextValue(strings.ToUpper(strconv.FormatInt(int64(number), int(t))))
	}
	return NewTextValue(strings.ToUpper(strconv.FormatUint(number, int(t))))
}

func mysqlFormatFunction(s *Schema, args []Value) (Value, error) {
	if args[0].Null || args[1].Null {
		return NewNullValue(), nil
	}
	places := max(int64(0), min(int64(30), mysqlFunctionInt(args[1])))
	v := mysqlFunctionNumber(args[0])
	n, scale, err := valueDecimal(v)
	if err != nil {
		return NewNullValue(), err
	}
	n = roundDecimal(n, scale, int(places))
	text := decimalText(n, int(places))
	locale := "en_US"
	if len(args) > 2 && !args[2].Null {
		locale = mysqlStringValue(args[2])
	}
	decimal, group := ".", ","
	groupWidth := 3
	supported := true
	// Locale punctuation/grouping checked independently with Oracle MySQL 8.
	switch strings.ToLower(locale) {
	case "ar_ae", "ar_bh", "ar_dz", "ar_eg", "ar_in", "ar_iq", "ar_jo", "ar_kw", "ar_lb", "ar_ly", "ar_ma", "ar_om", "ar_qa", "ar_sd", "ar_sy", "ar_tn", "ar_ye", "en_au", "en_ca", "en_gb", "en_nz", "en_ph", "en_us", "en_za", "en_zw", "es_do", "es_gt", "es_hn", "es_mx", "es_ni", "es_pa", "es_pe", "es_pr", "es_sv", "es_us", "gu_in", "he_il", "hi_in", "ja_jp", "ko_kr", "ms_my", "th_th", "ur_pk", "zh_cn", "zh_hk", "zh_tw":
	case "ar_sa", "sr_rs":
		group = ""
	case "be_by", "da_dk", "de_be", "de_de", "de_lu", "es_ar", "es_bo", "es_cl", "es_co", "es_ec", "es_es", "es_py", "es_uy", "es_ve", "fo_fo", "hu_hu", "id_id", "is_is", "lt_lt", "mn_mn", "nb_no", "no_no", "ro_ro", "ru_ua", "sq_al", "tr_tr", "uk_ua", "vi_vn":
		decimal, group = ",", "."
	case "bg_bg":
		decimal, group = ",", "\x00"
	case "ca_es", "de_at", "el_gr", "eu_es", "fr_be", "fr_ca", "fr_ch", "fr_fr", "fr_lu", "gl_es", "hr_hr", "it_it", "nl_be", "nl_nl", "pl_pl", "pt_br", "pt_pt", "sl_si":
		decimal, group = ",", ""
	case "cs_cz", "es_cr", "et_ee", "fi_fi", "lv_lv", "mk_mk", "ru_ru", "sk_sk", "sv_fi", "sv_se":
		decimal, group = ",", " "
	case "de_ch":
		decimal, group = ".", "'"
	case "en_in", "ta_in", "te_in":
		groupWidth = 2
	case "it_ch", "rm_ch":
		decimal, group = ",", "'"
	default:
		supported = false
	}
	if !supported {
		mysqlNumericWarning(s, 1649, "Unknown locale: '"+locale+"'")
	}
	integer, fraction, _ := strings.Cut(text, ".")
	sign := ""
	if strings.HasPrefix(integer, "-") {
		sign = "-"
		integer = integer[1:]
	}
	if err := mysqlFunctionSize(s, uint64(len(text)+len(integer)/groupWidth*len(group)+8)); err != nil {
		return NewNullValue(), err
	}
	var result strings.Builder
	result.WriteString(sign)
	for i, char := range integer {
		remaining := len(integer) - i
		if i > 0 && remaining >= 3 && (remaining == 3 || (remaining-3)%groupWidth == 0) {
			result.WriteString(group)
		}
		result.WriteRune(char)
	}
	if places > 0 {
		result.WriteString(decimal)
		result.WriteString(fraction)
	}
	return NewTextValue(result.String()), nil
}
