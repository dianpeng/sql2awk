# -----------------------------------------------------------------------------
# AWK CSV parser, note this implementation does standard conforming parsing
# which respect the quoted string, but it is not performant at all ... One
# should call this function *line by line* for awk parsing. It is designed
# to be used to help sql2awk to support CSV/TSV format
# -----------------------------------------------------------------------------

function xsv_unquote(input, start, i, char, nchar, val) {
  _XSV_VAL = "";
  _XSV_ERR = "";

  len = length(input);
  val = "";

  for (i = start; i <= len; i++) {
    char = substr(input, i, 1);
    if (char == "\"" || char == "'") {
      # we are done here, break the loop
      _XSV_VAL = val;
      return i + 1;
    }

    if (char == "\\") {
      # escape sequences
      if (i + 1 > len) {
        _XSV_ERR = "invalid quoted string, not closed properly";
        _XSV_VAL = val;
        return i;
      }

      ncahr = substr(input, i+1, 1);
      if (nchar == "n") {
        val = val "\\n";
        i++;
      } else if (nchar == "t") {
        val = val "\\t";
      } else if (nchar == "b") {
        val = val "\\b";
      } else if (nchar == "r") {
        val = val "\\r";
      } else if (nchar == "v") {
        val = val "\\v";
      } else if (nchar == "\\") {
        val = val "\\";
      } else if (nchar == "'") {
        val = val "'";
      } else if (nchar == "\"") {
        val = val "\"";
      } else {
        _XSV_ERR = "invalid quoted string, unknown escape character";
        _XSV_VAL = val;
        return i;
      }
    } else {
      val = val char;
    }
  }


  _XSV_ERR = "invalid quoted string, not closed prolery";
  _XSV_VAL = val;
  return i;
}

function xsv_parse_line(line, pos, delim, out, i, len, field, val) {
  line  = substr(line, pos, length(line) - pos);
  len   = length(line);
  field = 0;
  val   = "";

  # notes awk's function index start from 1, not 0
  for (i = 1; i <= len; i++) {
    char = substr(line, i, 1);

    if (char == delim) {
      # a field has done, just flush the value out
      field++;
      out[field] = val;
      val = "";
    } else if (char == "\"" || char == "'") {
      # trying to lex a quoted string
      i = xsv_unquote(line, i+1) - 1;

      # save the parsed value into the corresponding field count
      field++;
      out[field] = _XSV_VAL;
      val = "";
    } else {
      # normal field character, just append it into the *val*
      val = val char;
    }
  }

  # the last field of line is not flushed
  field++;
  out[field] = val;

  return field;
}
