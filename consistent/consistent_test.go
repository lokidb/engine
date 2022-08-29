package consistent

import "testing"

func TestAddMemberAllReadyExist(t *testing.T) {
	ring, _ := New(5)

	err := ring.AddMember("a")
	if err != nil {
		t.Fatal(err)
	}

	err = ring.AddMember("a")
	if err == nil {
		t.Error("expecting error for already added member")
	}
}

func TestAddMemberTooManyMembers(t *testing.T) {
	ring, _ := New(1)

	err := ring.AddMember("a")
	if err != nil {
		t.Fatal(err)
	}

	err = ring.AddMember("b")
	if err == nil {
		t.Error("expecting error for too many members")
	}
}

func TestAddMemberInvalidMember(t *testing.T) {
	ring, _ := New(1)

	err := ring.AddMember("")
	if err == nil {
		t.Error("expecting error for empty member")
	}

	err = ring.AddMember("$b")
	if err == nil {
		t.Error("expecting error for using invalid prefix")
	}
}

func TestGetMember(t *testing.T) {
	ring, _ := New(100)

	err := ring.AddMember("a")
	if err != nil {
		t.Fatal(err)
	}

	err = ring.AddMember("b")
	if err != nil {
		t.Fatal(err)
	}

	member := ring.GetMemberForKey("key")
	if member != "a" && member != "b" {
		t.Error("expecting member a or b")
	}
}

func TestSmallRing(t *testing.T) {
	_, err := New(0)
	if err == nil {
		t.Error("expecting error for ring size 0")
	}

	ring, _ := New(1)

	err = ring.AddMember("a")
	if err != nil {
		t.Fatal(err)
	}

	member := ring.GetMemberForKey("key")
	if member != "a" {
		t.Error("expecting member a")
	}
}
