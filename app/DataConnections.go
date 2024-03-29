package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

const settingsFilename = "connection.json"

type Json struct {
	ID       int    `json:"id"`
	PID      int    `json:"pid"`
	URL      string `json:"url"`
	ShortURL string `json:"shortURL"`
	SourceIP string `json:"sourceIP"`
	Time     string `json:"time"`
	Count    int    `json:"count"`
}

func main() {
	fmt.Println("Сервер статистики запущен")
	ln, lnErr := net.Listen("tcp", "localhost:1111")
	if lnErr != nil {
		fmt.Println("Ошибка при запуске сервера:", lnErr)
		return
	}
	defer ln.Close()

	for {
		conn, connErr := ln.Accept()
		if connErr != nil {
			fmt.Println("Ошибка при принятии соединения:", connErr)
			continue
		}

		go statisticsConnection(conn)
	}
}

func statisticsConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		input := scanner.Text()
		args := strings.Fields(input)
		if args[0] == "1" {
			statConnections(args[1], args[2], args[3])
		} else if args[0] == "2" {
			var detailing []string
			for i := 1; i < len(args); i++ {
				detailing = append(detailing, args[i])
			}
			fmt.Println(detailing)
			response, err := os.ReadFile(settingsFilename)
			if err != nil {
				fmt.Println("Ошибка при чтении json-файла:", err)
				return
			}

			JsonFile := ByteToJSON(response)

			jsonData := createReport(detailing, JsonFile)

			err = writeJSONToFile(jsonData, "report.json")
			if err != nil {
				fmt.Println("Ошибка записи в файл:", err)
				return
			}
			_, statErr := conn.Write([]byte("1\n"))
			if statErr != nil {
				fmt.Println(statErr)
			}
		}
	}
}

func statConnections(url, shortURL, ip string) {

	parentConnect := Json{
		URL:      url,
		ShortURL: shortURL,
		Count:    1,
	}

	newConnect := Json{
		SourceIP: ip,
		Time:     time.Now().Format("2006-01-02 15:04"),
		Count:    1,
	}

	connections, err := readConnectionsFromFile()
	if err != nil {
		fmt.Println("Ошибка чтения из файла:", err)
		return
	}

	if connections == nil {
		connections = []Json{}
	}

	parentConnect.ID = generateUniqueID(connections)
	if UniqueParents(connections, parentConnect.URL) == true {
		connections = append(connections, parentConnect)
	} else {
		ParentsCount(connections, parentConnect.URL)
	}

	newConnect.ID = generateUniqueID(connections)
	newConnect.PID = generatePID(connections, url)
	connections = append(connections, newConnect)

	err = writeConnectionsToFile(connections)
	if err != nil {
		fmt.Println("Ошибка записи в файл:", err)
		return
	}

}

func ByteToJSON(file []byte) []Json {
	var Connections []Json

	if len(file) == 0 {
		return nil
	}

	err := json.Unmarshal(file, &Connections)
	if err != nil {
		return nil
	}

	return Connections
}

func readConnectionsFromFile() ([]Json, error) {
	var Connections []Json

	file, err := os.ReadFile(settingsFilename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	if len(file) == 0 {
		return nil, nil
	}

	err = json.Unmarshal(file, &Connections)
	if err != nil {
		return nil, err
	}

	return Connections, nil
}

func writeConnectionsToFile(workers []Json) error {
	jsonData, err := json.MarshalIndent(workers, "", "  ")
	if err != nil {
		return err
	}

	err = os.WriteFile(settingsFilename, jsonData, 0644)
	if err != nil {
		return err
	}

	return nil
}

func UniqueParents(Connections []Json, url string) bool {
	for _, connect := range Connections {
		if connect.URL == url {
			return false
		}
	}
	return true
}

func ParentsCount(Connections []Json, url string) {
	for index := range Connections {
		if Connections[index].URL == url {
			Connections[index].Count++
			return
		}
	}
}

func generateUniqueID(Connections []Json) int {
	maxID := 0
	for _, connect := range Connections {
		if connect.ID > maxID {
			maxID = connect.ID
		}
	}
	return maxID + 1
}

func generatePID(Connections []Json, url string) int {
	PID := 0
	for _, connect := range Connections {
		if connect.URL == url {
			PID = connect.ID
		}
	}
	return PID
}

func writeJSONToFile(data interface{}, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(data); err != nil {
		return err
	}

	return nil
}

func findURLByID(id int, connections []Json) string {
	for _, conn := range connections {
		if conn.ID == id {
			return conn.URL
		}
	}
	return ""
}

func findShortURLByID(id int, connections []Json) string {
	for _, conn := range connections {
		if conn.ID == id {
			return conn.ShortURL
		}
	}
	return ""
}
func createReport(detailing []string, connections []Json) map[string]interface{} {
	report := make(map[string]interface{})

	for _, connection := range connections {
		if connection.PID == 0 {
			continue
		}

		ip := connection.SourceIP
		Time := connection.Time[11:]
		shortURL := findShortURLByID(connection.PID, connections)
		url := findURLByID(connection.PID, connections) + " (" + shortURL + ")"

		currLevel := report
		for _, level := range detailing {

			if level == "SourceIP" {
				if _, ok := currLevel[ip]; !ok {
					currLevel[ip] = make(map[string]interface{})
					if _, ok := currLevel["Sum"]; !ok {
						currLevel["Sum"] = 0
					}
				}
				currLevel = currLevel[ip].(map[string]interface{})
			} else if level == "TimeInterval" {
				if _, ok := currLevel[Time]; !ok {
					currLevel[Time] = make(map[string]interface{})
					if _, ok := currLevel["Sum"]; !ok {
						currLevel["Sum"] = 0
					}
				}
				currLevel = currLevel[Time].(map[string]interface{})
			} else if level == "URL" {
				if _, ok := currLevel[url]; !ok {
					currLevel[url] = make(map[string]interface{})
					if _, ok := currLevel["Sum"]; !ok {
						currLevel["Sum"] = 0
					}
				}
				currLevel = currLevel[url].(map[string]interface{})
			}

			if _, ok := currLevel["Sum"]; !ok {
				currLevel["Sum"] = 0
			}
			currLevel["Sum"] = currLevel["Sum"].(int) + 1
		}
	}

	delete(report, "Sum")

	return report
}
