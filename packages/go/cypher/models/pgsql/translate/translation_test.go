package translate_test

import (
	"cuelang.org/go/pkg/regexp"
	"embed"
	"fmt"
	"github.com/specterops/bloodhound/cypher/frontend"
	"github.com/specterops/bloodhound/cypher/models/pgsql"
	"github.com/specterops/bloodhound/cypher/models/pgsql/format"
	"github.com/specterops/bloodhound/cypher/models/pgsql/translate"
	"github.com/specterops/bloodhound/dawgs/graph"
	"github.com/stretchr/testify/require"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

var (
	NodeKind1 = graph.StringKind("NodeKind1")
	NodeKind2 = graph.StringKind("NodeKind2")
	EdgeKind1 = graph.StringKind("EdgeKind1")
	EdgeKind2 = graph.StringKind("EdgeKind2")
)

type kindMappings map[graph.Kind]int16

func newKindMapper() pgsql.KindMapper {
	return kindMappings{
		NodeKind1: 1,
		NodeKind2: 2,
		EdgeKind1: 11,
		EdgeKind2: 12,
	}
}

func (s kindMappings) MapKinds(kinds graph.Kinds) ([]int16, graph.Kinds) {
	var (
		ids          = make([]int16, 0, len(kinds))
		missingKinds = make(graph.Kinds, 0, len(kinds))
	)

	for _, kind := range kinds {
		if kindID, hasKind := s[kind]; hasKind {
			ids = append(ids, kindID)
		} else if !missingKinds.ContainsOneOf(kind) {
			missingKinds = append(missingKinds, kind)
		}
	}

	return ids, missingKinds
}

//go:embed translation_cases/*
var testCaseFiles embed.FS

type Case struct {
	Name   string
	Cypher string
	PgSQL  string
}

func (s *Case) Reset() {
	s.Cypher = ""
}

func (s *Case) Copy() *Case {
	return &Case{
		Name:   s.Name,
		Cypher: s.Cypher,
		PgSQL:  s.PgSQL,
	}
}

func (s *Case) Assert(t *testing.T, expectedSQL string, kindMapper pgsql.KindMapper) {
	if regularQuery, err := frontend.ParseCypher(frontend.NewContext(), s.Cypher); err != nil {
		t.Fatalf("Failed to compile cypher translatedQuery: %s - %v", s.Cypher, err)
	} else if translatedStatements, err := translate.Translate(regularQuery, kindMapper, true); err != nil {
		t.Fatalf("Failed to translate cypher translatedQuery: %s - %v", s.Cypher, err)
	} else if formattedQuery, err := format.Translated(translatedStatements); err != nil {
		t.Fatalf("Failed to format SQL translatedQuery: %v", err)
	} else {
		require.Equalf(t, expectedSQL, formattedQuery, "Test case for cypher translatedQuery: '%s' failed to match.", s.Cypher)
	}
}

type CaseFile struct {
	path    string
	content []byte
}

func (s *CaseFile) Load(t *testing.T) ([]*Case, bool) {
	const (
		casePrefix      = "case:"
		exclusivePrefix = "exclusive:"
	)

	var (
		testCases         []*Case
		isExclusive       = false
		hasExclusiveTests = false
		nextTestCase      = &Case{}
		queryBuilder      = strings.Builder{}
	)

	for _, line := range strings.Split(string(s.content), "\n") {
		// Crush unnecessary whitespace
		formattedLine, err := regexp.ReplaceAll("\\s+", strings.TrimSpace(line), " ")
		require.Nilf(t, err, "error while attempting to collapse whitespace in query")

		if len(formattedLine) == 0 {
			continue
		}

		if isLineComment := strings.HasPrefix(formattedLine, "--"); isLineComment {
			// Strip the comment header
			formattedLine = strings.Trim(formattedLine, "- ")

			lowerFormattedLine := strings.ToLower(formattedLine)

			if caseIndex := strings.Index(lowerFormattedLine, casePrefix); caseIndex != -1 {
				// This is a new test case - capture the comment as the cypher statement to test
				nextTestCase.Cypher = strings.TrimSpace(formattedLine[caseIndex+len(casePrefix):])
			} else if strings.Contains(lowerFormattedLine, exclusivePrefix) {
				if !hasExclusiveTests {
					// Clear the existing test cases
					testCases = testCases[:0]
					hasExclusiveTests = true
				}

				// The current test case as being marked as run-only
				isExclusive = true
			}
		} else if len(nextTestCase.Cypher) > 0 {
			// Strip any comment fragments for this line. Best effort; probably better done with a regex.
			if inlineCommentIdx := strings.Index(formattedLine, "--"); inlineCommentIdx >= 0 {
				formattedLine = strings.TrimSpace(formattedLine[:inlineCommentIdx])
			}

			// Check to make sure there's translatedQuery content.
			if len(formattedLine) == 0 {
				continue
			}

			// If there's content in the translatedQuery builder, prepend a space to conjoin the lines
			if queryBuilder.Len() > 0 {
				queryBuilder.WriteRune(' ')
			}

			queryBuilder.WriteString(formattedLine)

			// SQL queries must end with a ';' character
			if strings.HasSuffix(formattedLine, ";") {
				nextTestCase.PgSQL = queryBuilder.String()

				// Format the expected SQL translation and create a sub-test
				if isExclusive || !hasExclusiveTests {
					nextTestCase.Name = filepath.Base(s.path) + " " + nextTestCase.Cypher
					testCases = append(testCases, nextTestCase.Copy())
				}

				// Reset the query builder and test case
				queryBuilder.Reset()
				nextTestCase.Reset()

				isExclusive = false
			}
		}
	}

	return testCases, hasExclusiveTests
}

func ReadCaseFile(path string, fin fs.File) (CaseFile, error) {
	content, err := io.ReadAll(fin)

	return CaseFile{
		path:    path,
		content: content,
	}, err
}

func TestTranslate(t *testing.T) {
	var (
		caseFiles  []CaseFile
		kindMapper = newKindMapper()
	)

	require.Nil(t, fs.WalkDir(testCaseFiles, "translation_cases", func(path string, dir fs.DirEntry, err error) error {
		if !dir.IsDir() {
			if strings.HasSuffix(path, ".sql") {
				if fin, err := testCaseFiles.Open(path); err != nil {
					return err
				} else {
					defer fin.Close()

					if caseFile, err := ReadCaseFile(path, fin); err != nil {
						return err
					} else {
						caseFiles = append(caseFiles, caseFile)
					}
				}
			}
		}

		return nil
	}))

	var (
		casesRun          = 0
		hasExclusiveTests = false
		testCases         []*Case
	)

	for _, caseFile := range caseFiles {
		loadedTestCases, caseFileHasExclusiveTests := caseFile.Load(t)

		if !hasExclusiveTests {
			if caseFileHasExclusiveTests {
				hasExclusiveTests = true
				testCases = testCases[:0]
			}

			testCases = append(testCases, loadedTestCases...)
		} else if caseFileHasExclusiveTests {
			testCases = append(testCases, loadedTestCases...)
		}
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			testCase.Assert(t, testCase.PgSQL, kindMapper)
		})

		casesRun += 1
	}

	fmt.Printf("Ran %d test cases\n", casesRun)
}
