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
function agg_histogram(input, input_start, input_size, minval, maxval, numbin,
                       osep, step, cur, bin, i, v, j) {
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

# not very accurate indeed
function is_decimal(v) { return (v - int(v)) != 0.0 }

function is_integer(v) { return is_number(v) && !is_decimal(v); }

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

function type(v) { return typeof(v); }
function is_empty(v) { return length(v) == 0; }
function clear_array(x) { split("", x); }
function kv_make(k, v) { return sprintf("%s:%s", k, v); }
function kv_getv(kv, lv) {
  split(kv, lv, ":");
  return lv[2];
}

function asorti_rev(input, out, tmp_out, local_l, i) {
  local_l = asorti(input, tmp_out);
  clear_array(out);
  for (i = local_l; i > 0; i--) {
    out[(local_l-i)+1] = tmp_out[i];
  }
  return local_l;
}

# ------------------------------------------------------------------------
#
# Notes, the following function can be used by user's SQL
#   The sql analyzer will rewrite any possible function call into prefixed
#   version which allows user to call runtime function correctly
#
# ------------------------------------------------------------------------
function sql2awk_is_decimal(v) { return is_decimal(v); }
function sql2awk_is_integer(v) { return is_integer(v); }
function sql2awk_is_number(v) { return is_number(v); }
function sql2awk_is_string(v) { return is_string(v); }
function sql2awk_is_empty(v) { return is_empty(v); }
function sql2awk_type(v) { return type(v); }
function sql2awk_cast(v, ty) { return cast(v, ty); }

function sql2awk_length(v) { return length(v); }
function sql2awk_to_lower(v) { return tolower(v); }
function sql2awk_to_upper(v) { return toupper(v); }
function sql2awk_substr(a, b, c) { return substr(a, b, c); }

function sql2awk_math_cos(a) { return cos(a); }
function sql2awk_math_sin(a) { return sin(a); }
function sql2awk_math_sqrt(a) { return sqrt(a); }
function sql2awk_math_exp(a) { return exp(a); }
function sql2awk_math_int(a) { return int(a); }
function sql2awk_math_log(a) { return log(a); }
function sql2awk_math_atan2(a, b) { return atan2(a, b); }

function sql2awk_defval(v, defv) {
  if (is_empty(v)) {
    return defv;
  } else {
    return v;
  }
}
function sql2awk_if_empty(a, b) { return sql2awk_defval(a, b); }

function sql2awk_regexp_is_match(a, b) {
  return match(a, b) != 0;
}
