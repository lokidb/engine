// Limited file cursor for giving permissions on chunk of file
package cursor

import (
	"fmt"
	"io"
	"os"
)

type cursor struct {
	file      *os.File
	start_pos int64
	end_pos   int64
	curr_pos  int64
	length    int64
}

type Cursor interface {
	Seek(int64, int) (int64, error)
	Read(int) ([]byte, error)
}

func New(file *os.File, start int64, length int64) Cursor {
	c := new(cursor)
	c.file = file
	c.start_pos = start
	c.end_pos = c.start_pos + length
	c.curr_pos = start
	c.length = length

	return c
}

func (c *cursor) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		offset = c.start_pos + offset
	case io.SeekCurrent:
		offset = c.curr_pos + offset
	case io.SeekEnd:
		offset = c.end_pos + offset
	default:
		return 0, fmt.Errorf("invalid whence, supports [0, 1, 2]")
	}

	if offset > c.end_pos || offset < c.start_pos {
		return 0, fmt.Errorf("seek out of bound")
	}

	curr, err := c.file.Seek(offset, 0)
	if err != nil {
		return 0, err
	}

	c.curr_pos = curr

	return curr, nil
}

func (c *cursor) Read(offset int) ([]byte, error) {
	if c.curr_pos+int64(offset) > c.end_pos {
		return nil, fmt.Errorf("read out of bound")
	}

	data := make([]byte, offset)
	_, err := c.file.Read(data)
	if err != nil {
		return nil, err
	}

	return data, nil
}
