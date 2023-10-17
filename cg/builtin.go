package cg

const builtinAWK = `
function agg_percentile(arr, n,
                        local_sorted_value_size, i) {
  # sort the array based on its value
  local_sorted_value_size = asort(arr);
  i = int((n * local_sorted_value_size) / 100);
  i = i > local_sorted_value_size ? local_sorted_value_size : i;
  i = i <= 0 ? 1 : i;
  return arr[i""];
}

function order_key(v, local_typeof) {
  local_typeof = typeof(v);
  if (local_typeof == "strnum" ||
      local_typeof == "number" ||
      local_typeof == "number|bool") {
    if (is_decimal(v)) {
      return sprintf("%24f", (v+0.0));
    } else {
      return sprintf("%20d", (v+0));
    }
  } else {
    return v"";
  }
}

# helper to support histogram calculation in AWK
function agg_histogram(input,
                       input_start,
                       input_size,
                       minval,
                       maxval,
                       numbin, osep, step, cur, bin, i, v, j) {
  if (numbin <= 0 || (maxval < minval)) {
    return "[invalid input]";
  }

  step = (maxval - minval) / numbin;
  if (length(osep) == 0) {
    osep = ":";
  }

  # cleanup the bins
  for (i = 0; i <= numbin+1; i++) {
    bin[i] = 0;
  }

  for (i = input_start; i <= input_size; i++) {
    v = input[i""]; # value of the input
    cur = minval;

    for (j = 1; j <= numbin; j++) {
      if (v < cur) {
        # previous index is the one we are targeting
        j = j -1;
        break
      } else {
        # continue searching
        cur += step;
      }
    }

    bin[j]++;
  }

  # iterate through the *bin* to report the result
  output = array_join(bin, 1, numbin, osep);
  return sprintf("!%d%s%s%s!%d", bin[0], osep, output, osep, bin[numbin+1])
}

function array_join(array, start, end, sep,    result, i) {
	if (sep == "")
   sep = ";"
	result = array[start]
	for (i = start + 1; i <= end; i++)
    result = result sep array[i]
	return result
}

# type conversion and type assertion
function is_number(v, xx) {
  xx = typeof(v);
  return xx == "number" || xx == "strnum" || xx == "number|bool";
}

function is_decimal(v) {
  return (v - int(v)) != 0.0
}

function is_integer(v) {
  return is_number(v) && !is_decimal(v);
}

function is_string(v, xx) {
  xx = typeof(v);
  return xx == "string" || xx == "strnum";
}

function cast(v, ty) {
  if (ty == "int") {
    return int(v+0);
  } else if (ty == "float") {
    return v+0.0;
  } else if (ty == "string") {
    return v"";
  } else {
    return v;
  }
}

function type(v) {
  return typeof(v);
}

function is_null(v) {
  return length(v) == 0;
}

function clear_array(x) {
  split("", x);
}

function kv_make(k, v) {
  return sprintf("%s:%s", k, v);
}

function kv_getv(kv, lv) {
  split(kv, lv, ":");
  return lv[2];
}
`

const builtinGoAWK = `
# go's AWK does not support typeof
function typeof(obj,   q, x, z) {
  q = CONVFMT
  CONVFMT = "% g"
    split(" " obj "\1" obj, x, "\1")
    x[1] = obj == x[1]
    x[2] = obj == x[2]
    x[3] = obj == 0
    x[4] = obj "" == +obj
  CONVFMT = q
  z["0001"] = z["1101"] = z["1111"] = "number"
  z["0100"] = z["0101"] = z["0111"] = "string"
  z["1100"] = z["1110"] = "strnum"
  z["0110"] = "undefined"
  return z[x[1] x[2] x[3] x[4]]
}

# go's AWK does not support asort
function asort(a, b, c) {
}

function asorti(a, b, c) {
}
`
