package jsonpointer

import "testing"

func TestEncodeDecodeSegment(t *testing.T) {
	cases := []struct {
		in      string
		encoded string
		decoded string
	}{
		{in: "", encoded: "", decoded: ""},
		{in: "simple", encoded: "simple", decoded: "simple"},
		{in: "a/b", encoded: "a~1b", decoded: "a/b"},
		{in: "a~b", encoded: "a~0b", decoded: "a~b"},
		{in: "a~b/c", encoded: "a~0b~1c", decoded: "a~b/c"},
	}
	for _, tc := range cases {
		if got := EncodeSegment(tc.in); got != tc.encoded {
			t.Fatalf("EncodeSegment(%q) = %q, want %q", tc.in, got, tc.encoded)
		}
		if got := DecodeSegment(tc.encoded); got != tc.decoded {
			t.Fatalf("DecodeSegment(%q) = %q, want %q", tc.encoded, got, tc.decoded)
		}
	}
}

func TestNormalize(t *testing.T) {
	cases := []struct {
		in      string
		out     string
		wantErr bool
	}{
		{in: "", out: "", wantErr: false},
		{in: "/", out: "", wantErr: false},
		{in: "/a/b", out: "/a/b", wantErr: false},
		{in: " /a/b ", out: "/a/b", wantErr: false},
		{in: "a/b", out: "", wantErr: true},
	}
	for _, tc := range cases {
		got, err := Normalize(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("Normalize(%q) expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Normalize(%q) unexpected error: %v", tc.in, err)
		}
		if got != tc.out {
			t.Fatalf("Normalize(%q) = %q, want %q", tc.in, got, tc.out)
		}
	}
}

func TestJoin(t *testing.T) {
	cases := []struct {
		parent string
		child  string
		out    string
	}{
		{parent: "", child: "", out: ""},
		{parent: "", child: "a", out: "/a"},
		{parent: "/a", child: "", out: "/a"},
		{parent: "/a", child: "b", out: "/a/b"},
		{parent: "/a", child: "b/c", out: "/a/b~1c"},
		{parent: "/a", child: "b~c", out: "/a/b~0c"},
	}
	for _, tc := range cases {
		if got := Join(tc.parent, tc.child); got != tc.out {
			t.Fatalf("Join(%q,%q) = %q, want %q", tc.parent, tc.child, got, tc.out)
		}
	}
}

func TestSplit(t *testing.T) {
	cases := []struct {
		in      string
		out     []string
		wantErr bool
	}{
		{in: "", out: nil, wantErr: false},
		{in: "/", out: nil, wantErr: false},
		{in: "/a/b", out: []string{"a", "b"}, wantErr: false},
		{in: "/a~1b", out: []string{"a/b"}, wantErr: false},
		{in: "/a~0b", out: []string{"a~b"}, wantErr: false},
		{in: "a/b", out: nil, wantErr: true},
	}
	for _, tc := range cases {
		got, err := Split(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("Split(%q) expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Split(%q) unexpected error: %v", tc.in, err)
		}
		if len(got) != len(tc.out) {
			t.Fatalf("Split(%q) len=%d want %d", tc.in, len(got), len(tc.out))
		}
		for i := range got {
			if got[i] != tc.out[i] {
				t.Fatalf("Split(%q)[%d]=%q want %q", tc.in, i, got[i], tc.out[i])
			}
		}
	}
}

func TestJoinSplitRoundTrip(t *testing.T) {
	segments := []string{"a", "b/c", "d~e"}
	path := ""
	for _, seg := range segments {
		path = Join(path, seg)
	}
	got, err := Split(path)
	if err != nil {
		t.Fatalf("Split(%q) unexpected error: %v", path, err)
	}
	if len(got) != len(segments) {
		t.Fatalf("round trip length=%d want %d", len(got), len(segments))
	}
	for i := range got {
		if got[i] != segments[i] {
			t.Fatalf("round trip[%d]=%q want %q", i, got[i], segments[i])
		}
	}
}
