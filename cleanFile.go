package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"os"
	"strings"
)


// clean a string so it can be parsed
func cleanString(stringToClean string) string {
	return strings.ToValidUTF8(stringToClean, " ")
}

// open a file and dump it's contents line by line to a channel given a record length
func readFile(cleanQueue chan string, recordLength int) {

	defer close(cleanQueue)
	// created a buffered reader so we can use the neato ReadRune method
	fileReader := bufio.NewReader(os.Stdin)
	log.Printf("Begin readFile reclen=%d", recordLength)

	var eof = false
	for {
		if eof {
			break
		}
		var line string
		for i := 0; i < recordLength; i++ {
			tmpRune, _, err := fileReader.ReadRune()
			if err != nil {
				if err == io.EOF {
					eof = true
					// break out of our loop if eof
					break
				} else {
					// utf8 issue most likely
					tmpRune = ' '
				}
			}
			line = line + string(tmpRune)
		}
		cleanQueue <- line
	}
}

// write incoming lines to stdout
func writeRecords(outQueue chan string) {
	var count int64 = 0

	// created a buffered reader so we can use the neato ReadString method
	fileWriter := bufio.NewWriter(os.Stdout)

	for line := range outQueue {
		count = count + 1
		_, _ = fileWriter.WriteString(line)
		if count % 1000 == 0 {
			log.Print("1000 records")
		}
	}

	_ = fileWriter.Flush()
	_ = os.Stdout.Close()
}

func cleanRecords(cleanQueue chan string, outQueue chan string) {
	defer close(outQueue)
	for line := range cleanQueue {
		outQueue <- cleanString(line)
	}
}

// clean a record given its record length
func clean(recordLength int) {

	cleanQueue := make(chan string, 1000)
	outQueue := make(chan string, 1000)

	go cleanRecords(cleanQueue, outQueue)
	go readFile(cleanQueue, recordLength);
	writeRecords(outQueue);
}

func main() {

	log.SetOutput(os.Stderr)

	var recordLength = flag.Int("reclen", 0,"length of records")

	flag.Parse()
	log.Print("reclen=", *recordLength)

	if *recordLength == 0 {
		log.Printf("Missing Required Flag -recLen")
		flag.Usage()
		os.Exit(1)
	}

	clean(*recordLength)
}