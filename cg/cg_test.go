package cg

import (
	"fmt"
	gawki "github.com/benhoyt/goawk/interp"
	gawkp "github.com/benhoyt/goawk/parser"
	"github.com/dianpeng/sql2awk/plan"
	"github.com/dianpeng/sql2awk/sql"
	"github.com/stretchr/testify/assert"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"
)

const TEST_DIR = "./test"

// ---------------------------------------------------------------------------
// An automatic testing/verification tools for sql2awk. We use a simple text
// file as input to help us to perform verification of sql result. The file is
// simple text format with special tag to indicate what it is
//
// ## a line comment
// @![name of section]
// @!attr1: val1
// @!attr2: val2
// @@@@@@@@@@@@@@@@@@ (start of the raw content, notes at least 3 @ should exist)
// @================= (end of the raw content, notes at least 3 = should exist)

type cookbook struct {
	filename string
	parsed   sectionList
	input    []string
	code     string
	result   string
	awkType  int
}

type section struct {
	name         string
	attr         map[string]string
	content      string
	contentstart bool // used during parsing, not ideal
	contentbuf   []string
}

type sectionList []*section

func (self *section) addAttr(k, v string) {
	self.attr[k] = v
}

func (self *section) attrAt(k string) string {
	v, ok := self.attr[k]
	if ok {
		return v
	} else {
		return ""
	}
}

func (self *sectionList) get(n string) []*section {
	out := []*section{}
	for _, x := range *self {
		if x.name == n {
			out = append(out, x)
		}
	}
	return out
}

func (self *sectionList) getOne(n string) *section {
	x := self.get(n)
	if len(x) == 0 {
		return nil
	} else {
		return x[0]
	}
}

func (self *cookbook) charAt(
	x string,
	idx int,
) rune {
	if idx >= len(x) {
		return 0
	} else {
		r, _ := utf8.DecodeRuneInString(x[idx:])
		return r
	}
}

func (self *cookbook) idAt(
	x string,
	idx int,
) (string, int) {
	cursor := idx
	leading := true
	for cursor < len(x) {
		r, sz := utf8.DecodeRuneInString(x[cursor:])
		switch r {
		case ' ', '\r', '\b', '\n', '\t', '\v':
			if !leading {
				return x[idx:cursor], cursor
			}
			break
		default:
			if r == '_' || unicode.IsDigit(r) || unicode.IsLetter(r) {
				leading = false
				break
			} else {
				return x[idx:cursor], cursor
			}
		}
		cursor += sz
	}
	if leading {
		return "", -1
	} else {
		return x[idx:cursor], cursor
	}
}

func (self *cookbook) assignAt(
	x string,
	idx int,
) int {
	cursor := idx

	for cursor < len(x) {
		r, sz := utf8.DecodeRuneInString(x[cursor:])
		switch r {
		case ' ', '\r', '\b', '\n', '\t', '\v':
			break
		case '=', ':':
			return cursor + 1

		default:
			return -1
		}
		cursor += sz
	}
	return -1
}

func (self *cookbook) anyAt(
	x string,
	idx int,
) (string, int) {
	cursor := idx
	leading := true
	for cursor < len(x) {
		r, sz := utf8.DecodeRuneInString(x[cursor:])
		switch r {
		case ' ', '\r', '\b', '\n', '\t', '\v':
			if !leading {
				return x[idx:cursor], cursor
			}
			break
		default:
			leading = false
			break
		}
		cursor += sz
	}
	return x[idx:cursor], cursor
}

func (self *cookbook) parseLineMeta(
	l string,
	curSec *section,
) (bool, error) {
	switch self.charAt(l, 2) {
	case '[':
		if curSec.name != "" {
			return false, fmt.Errorf("section name already assigned")
		}
		pos := strings.Index(l, "]")
		if pos == -1 {
			return false, fmt.Errorf("section name should be closed by ]")
		}
		curSec.name = strings.TrimSpace(l[3:pos])
		return false, nil

	default:
		key, next := self.idAt(l, 2)
		if key == "" {
			return false, fmt.Errorf("expect an id for attribute")
		}
		next = self.assignAt(l, next)
		if next == -1 {
			return false, fmt.Errorf("expect an '=' for attribute assignment")
		}
		val, _ := self.anyAt(l, next)
		if val == "" {
			return false, fmt.Errorf("expect an value for attribute")
		}
		curSec.addAttr(key, val)
		return false, nil
	}
}

func (self *cookbook) parseLineContentStart(
	l string,
	curSec *section,
) (bool, error) {
	curSec.contentstart = true
	return false, nil
}

func (self *cookbook) parseLineContentEnd(
	l string,
	curSec *section,
) (bool, error) {
	curSec.contentstart = false
	curSec.content = strings.Join(curSec.contentbuf, "\n")
	return true, nil
}

func (self *cookbook) parseLine(
	l string,
	curSec *section,
) (bool, error) {
	l = strings.TrimSpace(l)

	switch self.charAt(l, 0) {
	default:
		if curSec.contentstart {
			curSec.contentbuf = append(curSec.contentbuf, l)
		}
		return false, nil
	case '@':
		nChar := self.charAt(l, 1)
		switch nChar {
		case '!':
			return self.parseLineMeta(l, curSec)
		case '@':
			// maybe content start
			return self.parseLineContentStart(l, curSec)
		case '=':
			return self.parseLineContentEnd(l, curSec)
		default:
			break
		}
	}
	return false, nil
}

func (self *cookbook) parse(
	data string,
) error {
	curSec := &section{
		attr: make(map[string]string),
	}

	for _, l := range strings.Split(data, "\n") {
		done, err := self.parseLine(l, curSec)
		if err != nil {
			return err
		}
		if done {
			self.parsed = append(self.parsed, curSec)
			curSec = &section{
				attr: make(map[string]string),
			}
		}
	}
	return nil
}

func (self *cookbook) parseFile() error {
	f, err := os.Open(self.filename)
	if err != nil {
		return fmt.Errorf("[parsing]: %s", err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("[parsing]: %s", err)
	}
	if err := self.parse(string(data)); err != nil {
		return fmt.Errorf("[parsing]: %s", err)
	}
	return nil
}

func saveToTmp(
	fn string,
	data string,
) error {
	return os.WriteFile(
		fn,
		[]byte(data),
		0644,
	)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func rndStr(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func saveToTmpRandom(
	data string,
) (string, error) {
	x := fmt.Sprintf("/tmp/%s.awk", rndStr(16))
	if err := saveToTmp(x, data); err != nil {
		return "", err
	} else {
		return x, nil
	}
}

func (self *cookbook) prepareTable() error {
	for _, x := range self.parsed.get("table") {
		if name := x.attrAt("name"); name != "" {
			if err := saveToTmp(
				name,
				x.content,
			); err != nil {
				return err
			} else {
				self.input = append(self.input, name)
			}
		}
	}
	return nil
}

func (self *cookbook) genAwk() error {
	yy := self.parsed.getOne("sql")
	if yy == nil {
		return fmt.Errorf("[plan]: sql is not found")
	}
	sqlCode := yy.content
	parser := sql.NewParser(sqlCode)
	c, err := parser.Parse()
	if err != nil {
		return fmt.Errorf("[plan]: %s", err)
	}

	self.awkType = AwkGnuAwk // system wise awk mostly
	if yy.attrAt("awk") == "goawk" {
		self.awkType = AwkGoAwk
	}
	p, err := plan.PlanCode(c)
	if err != nil {
		return fmt.Errorf("[plan]: %s", err)
	}

	code, err := Generate(p, &Config{
		OutputSeparator: " ",
		AwkType:         self.awkType,
	})
	if err != nil {
		return fmt.Errorf("[plan]: %s", err)
	}
	self.code = code

	if save := self.parsed.getOne("save"); save != nil {
		if path := save.attrAt("path"); path != "" {
			if err := saveToTmp(
				path,
				self.code,
			); err != nil {
				return err
			}
		}
	}
	if p := self.parsed.getOne("print"); p != nil {
		print(self.code, "\n")
	}
	return nil
}

func (self *cookbook) runGoAwk() error {
	prog, err := gawkp.ParseProgram(
		[]byte(self.code),
		nil,
	)
	if err != nil {
		return fmt.Errorf("[plan]: %s", err)
	}

	buf := strings.Builder{}
	interp, err := gawki.New(prog)
	if err != nil {
		return err
	}
	config := &gawki.Config{
		Output: &buf,
		Args:   self.input,
	}
	_, err = interp.Execute(config)
	if err != nil {
		return err
	}
	self.result = buf.String()
	return nil
}

func (self *cookbook) runSysAwk() error {
	awkPath := "/usr/bin/awk"
	awkFile, err := saveToTmpRandom(
		self.code,
	)
	if err != nil {
		return err
	}

	args := []string{
		"-f",
		awkFile,
	}
	args = append(args, self.input...)

	cmd := exec.Command(
		awkPath,
		args...,
	)
	stdout := &strings.Builder{}
	cmd.Stdout = stdout

	if err := cmd.Run(); err != nil {
		return err
	}

	self.result = stdout.String()
	return nil
}

func (self *cookbook) shouldRunAwk() bool {
	return self.parsed.getOne("result") != nil ||
		self.parsed.getOne("verify") != nil
}

func (self *cookbook) runAwk() error {
	if self.shouldRunAwk() {
		switch self.awkType {
		case AwkGoAwk:
			if err := self.runGoAwk(); err != nil {
				return err
			}
			break
		case AwkGnuAwk:
			if err := self.runSysAwk(); err != nil {
				return err
			}
			break
		default:
			return fmt.Errorf("unknown awk type")
		}
	}
	return nil
}

func (self *cookbook) run() error {
	if err := self.parseFile(); err != nil {
		return err
	}
	if err := self.prepareTable(); err != nil {
		return err
	}
	if err := self.genAwk(); err != nil {
		return err
	}
	if err := self.runAwk(); err != nil {
		return err
	}
	return self.verify()
}

func (self *cookbook) toOrderList(
	x string,
) [][]string {
	out := [][]string{}

	lines := strings.Split(x, "\n")
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		ll := []string{}
		for _, xx := range strings.Split(l, " ") {
			if v := strings.TrimSpace(xx); len(v) != 0 {
				ll = append(ll, v)
			}
		}
		out = append(out, ll)
	}
	return out
}

func (self *cookbook) cmpOrderList(
	l [][]string,
	r [][]string,
	order bool,
) error {
	if order {
		sort.Slice(
			l,
			func(i, j int) bool {
				lKey := strings.Join(l[i], "@")
				rKey := strings.Join(l[j], "@")
				return lKey < rKey
			},
		)
		sort.Slice(
			r,
			func(i, j int) bool {
				lKey := strings.Join(r[i], "@")
				rKey := strings.Join(r[j], "@")
				return lKey < rKey
			},
		)
	}

	if len(l) != len(r) {
		return fmt.Errorf("lhs rhs row size not match")
	}
	sz := len(l)
	for i := 0; i < sz; i++ {
		lhs := l[i]
		rhs := r[i]
		if len(lhs) != len(rhs) {
			return fmt.Errorf("row(%d) size not match", i)
		}
		lx := strings.Join(lhs, " ")
		rx := strings.Join(rhs, " ")

		if lx != rx {
			return fmt.Errorf("row(%d) ({%s} {%s}) not match", i, lx, rx)
		}
	}
	return nil
}

func (self *cookbook) verify() error {
	if yy := self.parsed.getOne("result"); yy != nil {
		order := true
		if v := yy.attrAt("order"); v == "none" {
			order = false
		}
		lhs := self.toOrderList(yy.content)
		rhs := self.toOrderList(self.result)
		if err := self.cmpOrderList(lhs, rhs, order); err != nil {
			return fmt.Errorf("[plan]: expect{\n%s\n}, result{\n%s\n}, failed: %s",
				yy.content,
				self.result,
				err,
			)
		}
	} else if yy := self.parsed.getOne("verify"); yy != nil {

		if x := yy.attrAt("size"); x != "" {
			if sz, err := strconv.Atoi(x); err != nil {
				return err
			} else {
				rList := self.toOrderList(self.result)
				if len(rList) != sz {
					return fmt.Errorf(
						"[plan]: verify(size) failed(%d != %d",
						len(rList),
						sz,
					)
				}
			}
		}

		if x := yy.attrAt("field"); x != "" {
			if sz, err := strconv.Atoi(x); err != nil {
				return err
			} else {
				rList := self.toOrderList(self.result)
				if len(rList) == 0 {
					return fmt.Errorf(
						"[plan]: verify(field) failed(empty table)",
					)
				}
				cSz := len(rList[0])

				if cSz != sz {
					return fmt.Errorf(
						"[plan]: verify(field) failed(%d != %d)",
						cSz,
						sz,
					)
				}
			}
		}
	}
	return nil
}

func TestCodeGen(t *testing.T) {
	assert := assert.New(t)
	dirList := []string{TEST_DIR}
	tt := 0
	ttErr := 0

	for len(dirList) > 0 {
		dir := dirList[0]
		dirList = dirList[1:]

		fList, err := os.ReadDir(
			dir,
		)
		assert.True(err == nil)

		for _, fentry := range fList {
			path := filepath.Join(dir, fentry.Name())
			if fentry.IsDir() {
				dirList = append(dirList, path)
				continue
			} else {
				cb := &cookbook{
					filename: path,
				}
				tt++
				if err := cb.run(); err != nil {
					print(fmt.Sprintf("cookbook(%s) failed: %s\n", path, err))
					assert.True(false)
					ttErr++
				} else {
					print(fmt.Sprintf("cookbook(%s) passed\n", path))
				}
			}
		}
	}

	t.Log(
		fmt.Sprintf(
			"total(%d), err(%d), ratio(%f)",
			tt,
			ttErr,
			float64(tt-ttErr)/float64(tt),
		),
	)
}
