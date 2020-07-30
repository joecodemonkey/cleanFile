package main

import (
	"flag"
	"io"
	"log"
	"os"
	"unicode/utf8"
)



// open a file and dump it's contents line by line to a channel given a record length
func readFile(cleanQueue chan []byte, recordLength int) {
	var count int64 = 1
	defer close(cleanQueue)

	log.Printf("Begin readFile reclen=%d", recordLength)

	var eof = false
	for {
		record := make([]byte, recordLength)
		if eof {
			break
		}
		bytesRead, err := os.Stdin.Read(record)

		if err != nil {
			if err == io.EOF {
				eof = true
				// break out of our loop if eof
				break
			} else {
				log.Printf("Read Error: %s", err.Error())
				break
			}
		}

		if bytesRead != recordLength {
			log.Printf("Expected %d bytes on read, got %d, possible odd reclen",
				recordLength, bytesRead)
		}

		cleanQueue <- record

		count = count + 1
	}
}

// write incoming lines to stdout
func writeRecords(outQueue chan []byte) {
	var count int64 = 0
	for line := range outQueue {
		count = count + 1
		_, _  = os.Stdout.Write(line)
		if count % 1000000 == 0 {
			log.Printf("%d records", count)
		}
	}
}

func closeFile(filePointer *os.File) {
	_ = filePointer.Close()
}

// clean a string so it can be parsed
func cleanString(bytesToClean []byte) []byte {
	// assuming 1 byte utf8, not safe, but probably good for fixed length
	// records anyway
	singleByteArray := make([]byte, 1)
	outArray := make([]byte, len(bytesToClean))
	for idx, value := range bytesToClean {
		singleByteArray[0] = value
		if(utf8.Valid(singleByteArray)) {
			outArray[idx] = singleByteArray[0]
		} else {
			outArray[idx] = ' '
		}

	}
	return outArray
}

// clean a record written to clean channels and put result on out channel
func cleanRecords(cleanQueue chan []byte, outQueue chan []byte, extractFile string) {
	var count int64 = 0

	var extractBad = false
	var extractFilePointer *os.File = nil
	if extractFile != "" {
		var err error
		extractFilePointer, err = os.Create(extractFile)
		if err != nil {
			log.Fatalf("Unable to open file [%s] - %s", extractFile, err.Error())
		}
		defer closeFile(extractFilePointer)

		extractBad = true
	}

	defer close(outQueue)

	for line := range cleanQueue {

		count += 1

		if utf8.Valid(line) {
			outQueue <- line
			continue
		}

		if extractBad {
			extractFilePointer.Write(line)
		}

		newLine := cleanString(line)
		log.Printf("Likely UTF8 Cleaning Error on Record: %d replacing with blank", count)
		outQueue <- newLine
	}
}

// primary clean function, gets channels setup and
// connects them
func clean(recordLength int, extractFile string) {

	cleanQueue := make(chan []byte, 1000)
	outQueue := make(chan []byte, 1000)

	go cleanRecords(cleanQueue, outQueue, extractFile)
	go readFile(cleanQueue, recordLength);
	writeRecords(outQueue);
}

func main() {

	log.SetOutput(os.Stderr)

	var recordLength = flag.Int("reclen", 0,"length of records")
	var extractFile = flag.String("extract", "","file to extract bad records to")

	flag.Parse()
	log.Print("reclen=", *recordLength)

	if *recordLength == 0 {
		log.Printf("Missing Required Flag -recLen")
		flag.Usage()
		os.Exit(1)
	}

	clean(*recordLength, *extractFile)
}