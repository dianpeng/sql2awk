# go's AWK does not support typeof
function typeof(obj,   q, x, z) {
	q = CONVFMT;
	CONVFMT = "% g";
	split(" " obj "\1" obj, x, "\1");
	x[1] = obj == x[1];
	x[2] = obj == x[2];
	x[3] = obj == 0;
	x[4] = obj "" == +obj;
	CONVFMT = q;
	z["0001"] = z["1101"] = z["1111"] = "number";
	z["0100"] = z["0101"] = z["0111"] = "string";
	z["1100"] = z["1110"] = "strnum";
	z["0110"] = "untyped";
	return z[x[1] x[2] x[3] x[4]];
}

# go's AWK does not support asort
function asort(a, b, c) {
	return 0;
}

function asorti(a, b, c) {
	return 0;
}
