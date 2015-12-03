package main

import (
	// "bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	MaxLinesPerFile = 256000
)

// var (
// 	// consumerGroup = flag.String("group", DefaultConsumerGroup, "The name of the consumer")
// 	linesPerFile = flag.Int("lines", MaxLinesPerFile, "The most lines you want per file")
// )

// max lines per file :: 25600

func main() {

	var fileName string
	var inputFolder string
	var outputFolder string
	var linesPerFile int
	// verbose = flag.Bool("verbose", false, "Turn on Sarama logging")
	flag.StringVar(&fileName, "file", "test.csv", "The file to parse")
	flag.StringVar(&inputFolder, "input", "/opt/osprey/data", "Where the files are going to come from")
	flag.StringVar(&outputFolder, "output", "/opt/osprey/data", "Where the files should end up")

	flag.IntVar(&linesPerFile, "lines", MaxLinesPerFile, "The most lines you want per file")

	flag.Parse()

	fmt.Println("INFO :: linesPerFile :: ", linesPerFile)

	// fmt
	csvfile, err := os.Open(path.Join(inputFolder, fileName))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer csvfile.Close()

	name_path := strings.Join([]string{inputFolder, fileName}, "/")
	name := strings.TrimSuffix(fileName, filepath.Ext(name_path))

	reader := csv.NewReader(csvfile)
	header, err := reader.Read()
	if err != nil {
		fmt.Println("ERR: ", err)
		return
	}
	fmt.Println(header)

	i := 0
	s := 0
	l := "0"

	f_name := strings.Join([]string{name, l, "csv"}, ".")
	st := []string{outputFolder, f_name}
	out_file_name := path.Join(outputFolder, f_name)
	fmt.Println("outfile :: ", out_file_name)
	out_file, err := os.OpenFile(out_file_name, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer out_file.Close()

	writer := csv.NewWriter(out_file)

	returnError := writer.Write(header) // []string("line,ts,Value,Col4,Col5")
	if returnError != nil {
		fmt.Println(returnError)
	}

	for {
		i += 1
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("ERR: ", err)
		} else {
			returnError := writer.Write(row) // []string("line,ts,Value,Col4,Col5")
			if returnError != nil {
				fmt.Println(returnError)
			}
			if i%linesPerFile == 0 {
				writer.Flush()

				// now create a new file!

				s += 1
				l = strconv.Itoa(s)
				f_name = strings.Join([]string{name, l, "csv"}, ".")
				st = []string{outputFolder, f_name}
				fmt.Println(strings.Join(st, "/"))
				fmt.Println(row)
				//
				out_file_name = path.Join(outputFolder, f_name)
				fmt.Println("outfile :: ", out_file_name)
				out_file, err = os.OpenFile(out_file_name, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
				if err != nil {
					fmt.Println(err)
					return
				}
				defer out_file.Close()

				writer = csv.NewWriter(out_file)

				returnError := writer.Write(header) // []string("line,ts,Value,Col4,Col5")
				if returnError != nil {
					fmt.Println(returnError)
				}

			}
		}
	}
	writer.Flush()
	fmt.Println(i)
	fmt.Println(s)
}
