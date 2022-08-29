package filestore

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/lokidb/engine/cursor"
)

func getValueFromPosition(file *os.File, itemPosition int64, valueReader func(cursor.Cursor) ([]byte, error)) ([]byte, error) {
	_, err := file.Seek(int64(itemPosition), io.SeekStart)
	if err != nil {
		return nil, err
	}

	itemHeader := make([]byte, itemHeaderLenght)
	_, err = file.Read(itemHeader)
	if err != nil {
		return nil, err
	}

	valuePosition, err := file.Seek(int64(itemHeader[0]), io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	valueLenght := binary.LittleEndian.Uint16(itemHeader[1:4])

	cur := cursor.New(file, valuePosition, int64(valueLenght))
	if valueReader != nil {
		return valueReader(cur)
	}

	value := make([]byte, valueLenght)
	_, err = file.Read(value)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func insertItemToFile(file *os.File, key string, value []byte) (int64, error) {
	// Jump to end of file
	itemPosition, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	// Item header is 32 int with 1 first byte as key size and 3 other bytes for total lenght
	itemHeader := make([]byte, 5)
	itemHeader[4] = 0
	binary.LittleEndian.PutUint32(itemHeader, (uint32(len(value))<<8)+uint32(len(key)))

	// create itemBytes from itemHeader and keyValue bytes
	item := append([]byte(key), value...)
	itemBytes := append(itemHeader, item...)

	_, err = file.Write(itemBytes)
	if err != nil {
		return 0, err
	}

	return itemPosition, nil
}

func markItemAsDeletedOnFile(file *os.File, itemPosition int64) error {
	// Jump to item header last byte (the byte that represent if the item is deleted)
	_, err := file.Seek(int64(itemPosition)+itemHeaderLenght-1, io.SeekStart)
	if err != nil {
		return err
	}

	// Overwrite first byte (key lenght) with 0 to mark as deleted
	file.Write([]byte{1})

	return nil
}

func scanFile(file *os.File, readValues bool, callback func(key string, value []byte, deleted bool, filePosition int64)) error {
	for {
		currentPosition, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		itemHeader := make([]byte, itemHeaderLenght)

		n, err := file.Read(itemHeader)
		if err != nil {
			if n == 0 {
				break
			} else {
				return err
			}
		}

		keyLenght := uint(itemHeader[0])
		valueLenght := binary.LittleEndian.Uint16(itemHeader[1:4])
		isDeletedFlag := uint(itemHeader[4])
		key := make([]byte, keyLenght)
		value := make([]byte, valueLenght)

		_, err = file.Read(key)
		if err != nil {
			return err
		}

		if readValues {
			_, err = file.Read(value)
			if err != nil {
				return err
			}
		}
		callback(string(key), value, isDeletedFlag == 1, currentPosition)

		if !readValues {
			_, err = file.Seek(int64(valueLenght), io.SeekCurrent)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
