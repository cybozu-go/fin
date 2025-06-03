package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/cybozu-go/fin/internal/infrastructure/sqlite"
	"github.com/cybozu-go/fin/internal/model"
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

func applyRawImageToFile(repo model.FinRepository, uid, rawImageFile, devicePath string) error {
	file, err := os.Open(rawImageFile)
	if err != nil {
		return err
	}
	defer file.Close()

	outputFile, err := os.OpenFile(devicePath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	const chunkSize = 10 * 1024 * 1024
	data, err := repo.GetActionPrivateData(uid)
	if err != nil {
		return err
	}
	var privateData restorePrivateData
	err = json.Unmarshal(data, &privateData)
	if err != nil {
		return err
	}
	for {
		data := make([]byte, chunkSize)
		_, err := file.ReadAt(data, privateData.NextRawImageChunk*chunkSize)
		if err != nil {
			if err == io.EOF {
				fmt.Println("Reached end of raw image file")
				break // End of file
			}
			return err
		}

		for _, b := range data {
			if b != 0 {
				_, err = outputFile.WriteAt(data, privateData.NextRawImageChunk*chunkSize)
				if err != nil {
					return fmt.Errorf("failed to write data at offset %d: %w", privateData.NextRawImageChunk*chunkSize, err)
				}
				break
			}
		}
		privateData.NextRawImageChunk++
		data, err = json.Marshal(privateData)
		if err != nil {
			return err
		}
		repo.UpdateActionPrivateData(uid, data)
	}
	return nil
}

type restorePrivateData struct {
	NextRawImageChunk int64  `json:"nextRawImageChunk,omitempty"`
	NextDiffPart      int    `json:"nextDiffPart,omitempty"`
	Phase             string `json:"phase,omitempty"`
}

func restore(targetSnapID, rawImageFile string, diffFiles []string, devicePath string) error {
	var repo model.FinRepository
	var err error
	repo, err = sqlite.New("fin.sqlite3?_txlock=immediate")
	if err != nil {
		return err
	}

	uid := "test-uid"
	err = repo.StartOrRestartAction(uid, model.Restore)
	if err != nil {
		fmt.Printf("failed to start action: %v\n", err)
		os.Exit(1)
	}

	rawSnapID := "snap1"
	diffSnapID := "snap2"

	if targetSnapID != rawSnapID && targetSnapID != diffSnapID {
		return fmt.Errorf("invalid target snap ID: %s, expected %s or %s", targetSnapID, rawSnapID, diffSnapID)
	}

	data, err := repo.GetActionPrivateData(uid)
	if err != nil {
		return err
	}
	var privateData restorePrivateData
	if len(data) != 0 {
		err = json.Unmarshal(data, &privateData)
		if err != nil {
			return err
		}
	}

	if privateData.Phase == "" {
		fmt.Println("Initial phase start")
		privateData.Phase = "discard"
		data, err := json.Marshal(privateData)
		if err != nil {
			return err
		}
		err = repo.UpdateActionPrivateData(uid, data)
		if err != nil {
			return err
		}
	}

	if privateData.Phase == "discard" {
		fmt.Printf("%s phase start\n", privateData.Phase)
		cmd := exec.Command("blkdiscard", "-f", devicePath)
		err = cmd.Run()
		if err != nil {
			return err
		}

		privateData.Phase = "restore_raw_image"
		data, err := json.Marshal(privateData)
		if err != nil {
			return err
		}
		err = repo.UpdateActionPrivateData(uid, data)
		if err != nil {
			return err
		}
	}

	if privateData.Phase == "restore_raw_image" {
		fmt.Printf("%s phase start\n", privateData.Phase)
		err = applyRawImageToFile(repo, uid, rawImageFile, devicePath)
		if err != nil {
			return err
		}

		if targetSnapID == rawSnapID {
			privateData.Phase = "completed"
		} else {
			privateData.Phase = "restore_diff"
		}
		data, err := json.Marshal(privateData)
		if err != nil {
			return err
		}
		err = repo.UpdateActionPrivateData(uid, data)
		if err != nil {
			return err
		}
	}

	if privateData.Phase == "restore_diff" {
		fmt.Printf("%s phase start\n", privateData.Phase)
		for i, diffFile := range diffFiles {
			err = applyDiffToFile(diffFile, devicePath)
			if err != nil {
				return err
			}
			privateData.NextDiffPart = i + 1
			data, err := json.Marshal(privateData)
			if err != nil {
				return err
			}
			err = repo.UpdateActionPrivateData(uid, data)
			if err != nil {
				return err
			}
			fmt.Printf("Applied diff file %s (%d/%d)\n", diffFile, i+1, len(diffFiles))
		}
		privateData.Phase = "completed"
		data, err := json.Marshal(privateData)
		if err != nil {
			return err
		}
		err = repo.UpdateActionPrivateData(uid, data)
		if err != nil {
			return err
		}
	}

	return repo.CompleteAction(uid)
}

func main() {
	if len(os.Args) != 5 {
		fmt.Printf("usage: %s target-snap-id(snap1 or snap2) <raw image file> <comma-separated diff file list> <output file>\n", os.Args[0])
		os.Exit(1)
	}

	targetSnapID := os.Args[1]
	rawImageFile := os.Args[2]
	diffFiles := strings.Split(os.Args[3], ",")
	devicePath := os.Args[4]

	err := restore(targetSnapID, rawImageFile, diffFiles, devicePath)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("restore completed successfully.")
}
