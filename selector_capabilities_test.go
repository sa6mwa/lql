package lql

import (
	"reflect"
	"testing"
)

func TestInspectSelectorCapabilitiesFamilies(t *testing.T) {
	selector, err := ParseSelectorString(`and.eq{field=/status,value=open},and.range{field=/progress,gte=5},or.in{field=/env,any=prod|stage},not.eq{field=/state,value=disabled},exists{/meta/etag},icontains{field=/msg,value=timeout},iprefix{field=/service,value=auth}`)
	if err != nil {
		t.Fatalf("ParseSelectorString: %v", err)
	}
	capabilities := InspectSelectorCapabilities(selector)
	if !capabilities.And || !capabilities.Or || !capabilities.Not || !capabilities.Eq || !capabilities.Range || !capabilities.In || !capabilities.Contains || !capabilities.Prefix || !capabilities.Exists {
		t.Fatalf("unexpected capability flags: %+v", capabilities)
	}
	families := capabilities.Families()
	want := []string{"and", "contains", "eq", "exists", "in", "not", "or", "prefix", "range"}
	if !reflect.DeepEqual(families, want) {
		t.Fatalf("families mismatch: got=%v want=%v", families, want)
	}
}

func TestInspectSelectorCapabilitiesPathComplexity(t *testing.T) {
	selector, err := ParseSelectorString(`/items[]/sku="A",/groups/**/sku="B",exists{/meta/.../etag}`)
	if err != nil {
		t.Fatalf("ParseSelectorString: %v", err)
	}
	capabilities := InspectSelectorCapabilities(selector)
	if !capabilities.WildcardPath {
		t.Fatalf("expected wildcard path capability")
	}
	if !capabilities.RecursivePath {
		t.Fatalf("expected recursive path capability")
	}
}

func TestInspectSelectorCapabilitiesEmpty(t *testing.T) {
	capabilities := InspectSelectorCapabilities(Selector{})
	if len(capabilities.Families()) != 0 {
		t.Fatalf("expected no families for empty selector, got %v", capabilities.Families())
	}
	if capabilities.WildcardPath || capabilities.RecursivePath {
		t.Fatalf("expected no path complexity for empty selector: %+v", capabilities)
	}
}
