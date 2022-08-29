package consistent

import (
	"fmt"
	"hash/crc64"
)

var crc64t = crc64.MakeTable(crc64.ISO)

const InvalidMemberPrefix = '$'

type empty struct{}

func hash(key []byte) uint64 {
	return crc64.Checksum(key, crc64t)
}

type ConsistentHash interface {
	AddMember(string) error
	GetMemberForKey(string) string
}

type consistentHash struct {
	array   []string
	size    uint64
	members map[string]empty
}

func New(ringSize uint64) (ConsistentHash, error) {
	if ringSize <= 0 {
		return nil, fmt.Errorf("ring size need to bigger then 0")
	}

	c := new(consistentHash)
	c.array = make([]string, ringSize)
	c.members = make(map[string]empty, ringSize)
	c.size = ringSize

	return c, nil
}

func (c *consistentHash) AddMember(member string) error {
	if member == "" || member[0] == InvalidMemberPrefix {
		return fmt.Errorf("can't have empty member or member starting with '%c'", InvalidMemberPrefix)
	}

	if (len(c.members) + 1) > int(c.size) {
		return fmt.Errorf("cant add more members then ring size")
	}

	if _, ok := c.members[member]; ok {
		return fmt.Errorf("member all ready exist")
	}

	c.members[member] = empty{}

	index := hash([]byte(member)) % c.size
	c.array[index] = string(InvalidMemberPrefix) + member

	i := index
	for {
		if i == 0 {
			i = c.size - 1
		} else {
			i--
		}

		if c.array[i] != "" && c.array[i][0] == InvalidMemberPrefix {
			break
		}

		c.array[i] = member
	}

	return nil
}

func (c *consistentHash) GetMemberForKey(key string) string {
	index := hash([]byte(key)) % c.size

	member := c.array[index]
	if member[0] == InvalidMemberPrefix {
		member = member[1:]
	}

	return member
}
