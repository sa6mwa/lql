package lql

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestParseProjectionPaths(t *testing.T) {
	paths, err := ParseProjectionPaths([]string{" /id ", "", "/meta/trace", "/id"})
	if err != nil {
		t.Fatalf("ParseProjectionPaths: %v", err)
	}
	if len(paths) != 2 {
		t.Fatalf("expected deduped path count 2, got %d", len(paths))
	}
	if paths[0].Raw != "/id" || paths[1].Raw != "/meta/trace" {
		t.Fatalf("unexpected paths: %#v", paths)
	}
}

func TestParseProjectionPathsValidation(t *testing.T) {
	if _, err := ParseProjectionPaths([]string{"/"}); err == nil {
		t.Fatalf("expected root path error")
	}
	if _, err := ParseProjectionPaths([]string{"/0/id"}); err == nil {
		t.Fatalf("expected leading index error")
	}
	if _, err := ParseProjectionPaths([]string{"", " "}); err == nil {
		t.Fatalf("expected empty path set error")
	}
}

func TestProjectFieldsBasic(t *testing.T) {
	paths, err := ParseProjectionPaths([]string{"/id", "/meta/trace", "/items/1/sku"})
	if err != nil {
		t.Fatalf("parse paths: %v", err)
	}
	input := []byte(`{"id":"a","meta":{"trace":9,"ignore":1},"items":[{"sku":"A"},{"sku":"B"}],"payload":"x"}`)
	var out bytes.Buffer
	result, err := ProjectFields(ProjectFieldsRequest{
		Reader: bytes.NewReader(input),
		Writer: &out,
		Paths:  paths,
	})
	if err != nil {
		t.Fatalf("ProjectFields: %v", err)
	}
	if !result.Found {
		t.Fatalf("expected found=true")
	}

	got, err := decodeJSONAny(out.Bytes())
	if err != nil {
		t.Fatalf("decode output: %v", err)
	}
	want := map[string]any{
		"id": "a",
		"meta": map[string]any{
			"trace": json.Number("9"),
		},
		"items": []any{
			nil,
			map[string]any{"sku": "B"},
		},
	}
	if !deepEqualJSONValue(got, want) {
		t.Fatalf("projection mismatch:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestProjectFieldsMissingReturnsNotFound(t *testing.T) {
	paths, err := ParseProjectionPaths([]string{"/missing"})
	if err != nil {
		t.Fatalf("parse paths: %v", err)
	}
	var out bytes.Buffer
	result, err := ProjectFields(ProjectFieldsRequest{
		Reader: bytes.NewReader([]byte(`{"id":"a"}`)),
		Writer: &out,
		Paths:  paths,
	})
	if err != nil {
		t.Fatalf("ProjectFields: %v", err)
	}
	if result.Found {
		t.Fatalf("expected found=false")
	}
	if out.Len() != 0 {
		t.Fatalf("expected empty output when not found")
	}
}

func TestProjectFieldsValidationAndErrors(t *testing.T) {
	paths, err := ParseProjectionPaths([]string{"/id"})
	if err != nil {
		t.Fatalf("parse paths: %v", err)
	}

	cases := []struct {
		name  string
		req   ProjectFieldsRequest
		code  StreamErrorCode
		check string
	}{
		{
			name: "non-object-root",
			req: ProjectFieldsRequest{
				Reader: bytes.NewReader([]byte(`7`)),
				Writer: &bytes.Buffer{},
				Paths:  paths,
			},
			code:  StreamErrorInvalidBody,
			check: "JSON objects",
		},
		{
			name: "invalid-json",
			req: ProjectFieldsRequest{
				Reader: bytes.NewReader([]byte(`{"id":`)),
				Writer: &bytes.Buffer{},
				Paths:  paths,
			},
			code: StreamErrorInvalidBody,
		},
		{
			name: "trailing-token",
			req: ProjectFieldsRequest{
				Reader: bytes.NewReader([]byte(`{"id":"a"} {"id":"b"}`)),
				Writer: &bytes.Buffer{},
				Paths:  paths,
			},
			code: StreamErrorInvalidBody,
		},
		{
			name: "missing-paths",
			req: ProjectFieldsRequest{
				Reader: bytes.NewReader([]byte(`{"id":"a"}`)),
				Writer: &bytes.Buffer{},
			},
			code: StreamErrorInvalidBody,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := mustProjectFieldsErr(tc.req)
			var streamErr *StreamError
			if !errors.As(err, &streamErr) {
				t.Fatalf("expected StreamError, got %T (%v)", err, err)
			}
			if streamErr.Code != tc.code {
				t.Fatalf("expected code %s, got %s", tc.code, streamErr.Code)
			}
			if tc.check != "" && !strings.Contains(streamErr.Error(), tc.check) {
				t.Fatalf("expected error to contain %q, got %v", tc.check, streamErr)
			}
		})
	}
}

func TestProjectFieldsPathConflict(t *testing.T) {
	paths, err := ParseProjectionPaths([]string{"/meta", "/meta/trace"})
	if err != nil {
		t.Fatalf("parse paths: %v", err)
	}
	_, err = ProjectFields(ProjectFieldsRequest{
		Reader: bytes.NewReader([]byte(`{"meta":{"trace":1}}`)),
		Writer: &bytes.Buffer{},
		Paths:  paths,
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorInvalidBody {
		t.Fatalf("expected invalid_body, got %s", streamErr.Code)
	}
}

func TestProjectFieldsSpoolAndCleanup(t *testing.T) {
	paths, err := ParseProjectionPaths([]string{"/payload"})
	if err != nil {
		t.Fatalf("parse paths: %v", err)
	}
	tempDir := t.TempDir()
	payload := strings.Repeat("x", 4096)
	input := []byte(`{"payload":"` + payload + `"}`)

	var out bytes.Buffer
	result, err := ProjectFields(ProjectFieldsRequest{
		Reader:           bytes.NewReader(input),
		Writer:           &out,
		Paths:            paths,
		SpoolMemoryBytes: 8,
		SpoolTempDir:     tempDir,
		SpoolFilePattern: "project-*.json",
	})
	if err != nil {
		t.Fatalf("ProjectFields: %v", err)
	}
	if !result.Found {
		t.Fatalf("expected found=true")
	}
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected spool cleanup, found %d files", len(entries))
	}
}

func TestProjectFieldsSpoolFailure(t *testing.T) {
	paths, err := ParseProjectionPaths([]string{"/payload"})
	if err != nil {
		t.Fatalf("parse paths: %v", err)
	}
	input := []byte(`{"payload":"` + strings.Repeat("x", 1024) + `"}`)
	_, err = ProjectFields(ProjectFieldsRequest{
		Reader:           bytes.NewReader(input),
		Writer:           &bytes.Buffer{},
		Paths:            paths,
		SpoolMemoryBytes: 8,
		SpoolTempDir:     t.TempDir() + "/missing",
		SpoolFilePattern: "project-*.json",
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorInvalidBody {
		t.Fatalf("expected invalid_body, got %s", streamErr.Code)
	}
}

func TestProjectFieldsMaxOutputBytes(t *testing.T) {
	paths, err := ParseProjectionPaths([]string{"/payload"})
	if err != nil {
		t.Fatalf("parse paths: %v", err)
	}
	input := []byte(`{"payload":"` + strings.Repeat("x", 256) + `"}`)
	_, err = ProjectFields(ProjectFieldsRequest{
		Reader:         bytes.NewReader(input),
		Writer:         &bytes.Buffer{},
		Paths:          paths,
		MaxOutputBytes: 32,
	})
	var streamErr *StreamError
	if !errors.As(err, &streamErr) {
		t.Fatalf("expected StreamError, got %T (%v)", err, err)
	}
	if streamErr.Code != StreamErrorDocumentTooLarge {
		t.Fatalf("expected document_too_large, got %s", streamErr.Code)
	}
}

func mustProjectFieldsErr(req ProjectFieldsRequest) error {
	_, err := ProjectFields(req)
	if err == nil {
		return errors.New("expected error")
	}
	return err
}

func deepEqualJSONValue(got, want any) bool {
	gotJSON, gotErr := json.Marshal(got)
	wantJSON, wantErr := json.Marshal(want)
	if gotErr != nil || wantErr != nil {
		return false
	}
	return bytes.Equal(gotJSON, wantJSON)
}

func legacyProjectFieldsForParity(input []byte, paths []ProjectionPath) ([]byte, bool, error) {
	decoded, err := decodeJSONAny(input)
	if err != nil {
		return nil, false, err
	}
	doc, ok := decoded.(map[string]any)
	if !ok {
		return nil, false, errors.New("field selection requires JSON objects")
	}
	root := any(map[string]any{})
	found := false
	for _, path := range paths {
		value, exists := legacyExtractProjectedValue(doc, path.Segments)
		if !exists {
			continue
		}
		found = true
		next, err := legacyAssignProjectedValue(root, path.Segments, value)
		if err != nil {
			return nil, false, err
		}
		root = next
	}
	if !found {
		return nil, false, nil
	}
	payload, err := json.Marshal(root)
	if err != nil {
		return nil, false, err
	}
	return payload, true, nil
}

func legacyExtractProjectedValue(root any, segments []string) (any, bool) {
	current := root
	for _, segment := range segments {
		switch node := current.(type) {
		case map[string]any:
			value, ok := node[segment]
			if !ok {
				return nil, false
			}
			current = value
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(node) {
				return nil, false
			}
			current = node[index]
		default:
			return nil, false
		}
	}
	return current, true
}

func legacyAssignProjectedValue(node any, path []string, value any) (any, error) {
	if len(path) == 0 {
		return value, nil
	}
	token := path[0]
	last := len(path) == 1
	if idx, err := strconv.Atoi(token); err == nil {
		if idx < 0 || idx > maxProjectionFieldIndex {
			return nil, errors.New("field index invalid")
		}
		var arr []any
		switch v := node.(type) {
		case nil:
			arr = make([]any, idx+1)
		case []any:
			arr = v
			if idx >= len(arr) {
				arr = append(arr, make([]any, idx-len(arr)+1)...)
			}
		default:
			return nil, errors.New("field path conflict")
		}
		if last {
			arr[idx] = value
			return arr, nil
		}
		next, err := legacyAssignProjectedValue(arr[idx], path[1:], value)
		if err != nil {
			return nil, err
		}
		arr[idx] = next
		return arr, nil
	}
	var obj map[string]any
	switch v := node.(type) {
	case nil:
		obj = make(map[string]any)
	case map[string]any:
		obj = v
	default:
		return nil, errors.New("field path conflict")
	}
	if last {
		obj[token] = value
		return obj, nil
	}
	next, err := legacyAssignProjectedValue(obj[token], path[1:], value)
	if err != nil {
		return nil, err
	}
	obj[token] = next
	return obj, nil
}
