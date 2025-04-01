package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

/*
 * manage rbd diff file in non-ceph environment
 * support only v1 format
 * 
 * diff file spec:
 * https://docs.ceph.com/en/reef/dev/rbd-diff/
 */

func applyDiffToFile(diffFilePath, outputFilePath string) error {
	diffFile, err := os.Open(diffFilePath)
	if err != nil {
		return fmt.Errorf("failed to open diff file: %w", err)
	}
	defer diffFile.Close()

	outputFile, err := os.OpenFile(outputFilePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open output file: %w", err)
	}
	defer outputFile.Close()

	headerBytes := make([]byte, 12)
	_, err = diffFile.Read(headerBytes)
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}
	header := string(headerBytes)
	if header != "rbd diff v1\n" {
		return fmt.Errorf("invalid header: %s", header)
	}

	for {
		tag := make([]byte, 1)
		_, err := diffFile.Read(tag)
		if err == io.EOF {
			break // End of file
		}
		if err != nil {
			return fmt.Errorf("failed to read tag: %w", err)
		}

		switch tag[0] {
		case 'f', 't': // FROM SNAP or TO SNAP
			kind := "FROM"
			if tag[0] == 't' {
				kind = "TO"
			}

			lengthBytes := make([]byte, 4)
			_, err = io.ReadFull(diffFile, lengthBytes)
			if err != nil {
				return fmt.Errorf("failed to read %s snap name length: %w", kind, err)
			}
			length := binary.LittleEndian.Uint32(lengthBytes)

			nameBytes := make([]byte, length)
			_, err = io.ReadFull(diffFile, nameBytes)
			if err != nil {
				return fmt.Errorf("failed to read %s snap name length: %w", kind, err)
			}
			s := string(nameBytes)
			fmt.Printf("%s SNAP: %s\n", kind, s)

		case 's': // SIZE
			sizeBytes := make([]byte, 8)
			_, err = io.ReadFull(diffFile, sizeBytes)
			if err != nil {
				return fmt.Errorf("failed to read size: %w", err)
			}
			size := binary.LittleEndian.Uint64(sizeBytes)
			fmt.Printf("SIZE: %d\n", size)

		case 'w': // UPDATED DATA
			offsetBytes := make([]byte, 8)
			lengthBytes := make([]byte, 8)
			_, err = io.ReadFull(diffFile, offsetBytes)
			if err != nil {
				return fmt.Errorf("failed to read offset: %w", err)
			}
			_, err = io.ReadFull(diffFile, lengthBytes)
			if err != nil {
				return fmt.Errorf("failed to read length: %w", err)
			}
			offset := binary.LittleEndian.Uint64(offsetBytes)
			length := binary.LittleEndian.Uint64(lengthBytes)

			data := make([]byte, length)
			_, err = io.ReadFull(diffFile, data)
			if err != nil {
				return fmt.Errorf("failed to read data: %w", err)
			}

			fmt.Printf("UPDATED DATA: offset %d, length %d\n", offset, length)
			break

			_, err = outputFile.WriteAt(data, int64(offset))
			if err != nil {
				return fmt.Errorf("failed to write data: %w", err)
			}

		case 'z': // ZERO DATA
			offsetBytes := make([]byte, 8)
			lengthBytes := make([]byte, 8)
			_, err = io.ReadFull(diffFile, offsetBytes)
			if err != nil {
				return fmt.Errorf("failed to read offset: %w", err)
			}
			_, err = io.ReadFull(diffFile, lengthBytes)
			if err != nil {
				return fmt.Errorf("failed to read length: %w", err)
			}
			offset := binary.LittleEndian.Uint64(offsetBytes)
			length := binary.LittleEndian.Uint64(lengthBytes)

			fmt.Printf("UPDATED DATA: offset %d, length %d\n", offset, length)
			break

			// Write zeros to the output file
			zeroData := make([]byte, length)
			_, err = outputFile.WriteAt(zeroData, int64(offset))
			if err != nil {
				return fmt.Errorf("failed to write zero data: %w", err)
			}

		case 'e': // END
			return nil

		default:
			return fmt.Errorf("unknown tag: %c", tag[0])
		}
	}

	return nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("usage: %s [-d] <diff file> <output file>\n", os.Args[0])
		os.Exit(1)
	}
	diffFile := os.Args[1]
	outFile := os.Args[2]

	err := applyDiffToFile(diffFile, outFile)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	} else {
		fmt.Println("diff applied successfully.")
	}
}
