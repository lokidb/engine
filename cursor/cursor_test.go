package cursor

import (
	"io"
	"os"
	"testing"
)

func TestPermissions(t *testing.T) {
	t.Cleanup(func() {
		os.Remove("test_cursor.tst")
	})

	file, err := os.Create("test_cursor.tst")
	if err != nil {
		t.Fatal(err)
	}

	file.WriteString("123456789qwertyuiopasdfghjklzxcvbnm")

	c := New(file, 5, 5)

	_, err = c.Read(6)
	if err == nil {
		t.Error("expecting error when accessing out of range bytes")
	}

	pos, err := c.Seek(0, io.SeekEnd)
	if err != nil {
		t.Error("expecting to be able to seek to the end of the cursor")
	}

	if pos != 10 {
		t.Errorf("expecting end of cursor to be 10 not %d", pos)
	}

	_, err = c.Seek(5, io.SeekEnd)
	if err == nil {
		t.Error("expecting to 'seek out of bound' error")
	}

	_, err = c.Seek(-20, io.SeekCurrent)
	if err == nil {
		t.Error("expecting to 'seek out of bound' error")
	}

	_, err = c.Seek(50, io.SeekCurrent)
	if err == nil {
		t.Error("expecting to 'seek out of bound' error")
	}

	_, err = c.Seek(-1, io.SeekStart)
	if err == nil {
		t.Error("expecting to 'seek out of bound' error")
	}

	_, err = c.Seek(10, io.SeekStart)
	if err == nil {
		t.Error("expecting to 'seek out of bound' error")
	}

	pos, err = c.Seek(0, io.SeekStart)
	if err != nil {
		t.Error("expecting seek to work")
	}

	if pos != 5 {
		t.Errorf("expecting position after seek to be 5 not %d", pos)
	}

	content, err := c.Read(1)
	if err != nil {
		t.Fatal(err)
	}

	if content[0] != '6' {
		t.Error("expecting first byte of cursor to be '6'")
	}
}
