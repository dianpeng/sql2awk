package cg

type awkGlobalFromFunc struct {
	funcName string
	g        []string
}

// collector used to flush awkWriter's global definition into. Used for us
// to clearly declare the global in the BEGIN scope of awk. This better
// for visualization of code generation and debug purpose.
type awkGlobal struct {
	G []awkGlobalFromFunc
}

// A special template engine used for *AWK* source code dump. We call it awk
// writer, but in fact it can do anything, TBH.

type awkWriter struct {
	g      *awkGlobalFromFunc // global from func collector
	indent int                // current indent level for formatting
	buf    *strings.Builder   // output buffer, notes, the function protocol is gen
	// when the body is done, since we need to collect
	// local count and local name
	local       []string        // locals
	localIndex  map[string]bool // used to dedup
	global      []string        // globals
	globalIndex map[string]bool // used to dedup

	paramPrefix string // parameter prefix
	param       int    // parameter #, same as *len of table scan list*. Notes all the
	// parameter for this function will be prefixed with fixed one
	funcName string // notes, AWK does not support nested function, or first order
	// function, we do not need to maintain multiple funcName,
	// if this field is "", we are in global scope
}

func (self *awkWriter) HasLocal(l string) bool {
	_, ok := self.localIndex[l]
	return ok
}

func (self *awkWriter) DefineLocal(
	n string,
) {
	self.Local(n)
}

func (self *awkWriter) DefineLocalN(
	n string,
	idx int,
) {
	self.LocalN(n, idx)
}

func (self *awkWriter) Local(
	n string,
) string {
	if !self.HasLocal(n) {
		self.local = append(self.local, n)
	}
	return n
}

func (self *awkWriter) LocalN(
	prefix string,
	idx int,
) string {
	n := fmt.Sprintf("%s_%d", prefix, idx)
	return self.Local(n)
}

func (self *awkWriter) HasGlobal(l string) bool {
	_, ok := self.globalIndex[l]
	return ok
}

func (self *awkWriter) RID(i int) string {
	if i >= self.param {
		panic("invalid RID")
	}
	return fmt.Sprintf("rid_%d", i)
}

// ----------------------------------------------------------------------------
//
// Substitution phase, ie the meta instruction inside of the template should
// be handled properly to allow certain things. Basically we allow 3 types of
// special substitution.
//
// 1. %[var0, var1, ..., %(lit), ..., varN] general substituion
// 2. @[instruction, ...]
// 3. #[special_known_name, ...] 
//
// Instruction is kind of special, it allows user to call certain special
// instruction of the writer, for example, to call pipeline function Next
//
// @[pipeline_next, %(group_by)]
//
// ----------------------------------------------------------------------------

type awkwriterCmd struct {
  cmd string // command's name
  arg []string // command's argument, after substituion
}

const (
  asciiPercent = 0x25
  asciiLPar = 0x28
  asciiRPar = 0x29
)

func parseAwkWriterCmd(
  input string,
  ctx map[string]interface{},
) (*awkwriterCmd, error) {
  x = strings.Split(input, ",")
  if len(x) == 0 {
    return nil, fmt.Errorf("invalid argument, at least one should be specified")
  }

  for idx, v := range x {
    x[idx] = strings.TrimSpace(v)
  }

  arg := x[1:]
  for idx, v := range arg {
    byteV := []byte(v)
    if len(byteV) > 1 &&
       byteV[0] == asciiPercent &&
       byteV[1] == asciiLPar &&
       byteV[len(v)-1] == asciiRPar {
      vv := byteV[2: len(v)-1]
      arg[idx] = string(vv)
    } else {
      // do the substitution
      if vvv, ok := ctx[v]; ok {
        arg[idx] = vvv
      } else {
        return nil, fmt.Errorf("variable(%s) is not found", v)
      }
    }
  }

  return &awkwriterCmd{
    cmd: x[0],
    arg: arg,
  }, nil
}

func (self *awkWriter) subLine(
  l string,
  ctx map[string]interface{},
) {
}





