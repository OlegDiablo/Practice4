package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
)

func shorting(url string) string {
	shortenHash := md5.Sum([]byte(url))
	shortLink := hex.EncodeToString(shortenHash[:3])
	return shortLink
}

func getIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			return ipnet.IP.String()
		}
	}

	return ""
}
func shortenURL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.ParseForm()
	url := r.FormValue("url")
	if url == "" {
		http.Error(w, "URL is required", http.StatusBadRequest)
		return
	}

	shortURL := shorting(url)
	conn, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		fmt.Println("Error accepting connection:", err)
		os.Exit(1)
	}
	defer conn.Close()
	_, send_err := conn.Write([]byte("add " + shortURL + " " + url + "\n"))
	if send_err != nil {
		fmt.Println(send_err)
	}

	fmt.Fprintf(w, "Shortened URL: localhost:8080/%s\n", shortURL)
}

func redirectURL(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	shortURL := strings.TrimPrefix(r.URL.Path, "/")

	DBconn, err := net.Dial("tcp", "localhost:6379")

	if err != nil {
		fmt.Println("Error accepting connection:", err)
		os.Exit(1)
	}
	_, send_err := DBconn.Write([]byte("get " + shortURL + "\n"))
	if send_err != nil {
		fmt.Println(send_err)
	}
	scanner := bufio.NewScanner(DBconn)
	scanner.Scan()
	url := scanner.Text()
	DBconn.Close()

	http.Redirect(w, r, url, http.StatusFound)

	statConn, err := net.Dial("tcp", "localhost:1111")
	if err != nil {
		fmt.Println("Error accepting connection:", err)
		os.Exit(1)
	}

	ip := getIP()
	_, stat_err := statConn.Write([]byte("1 " + url + " " + shortURL + " " + ip + "\n"))
	if stat_err != nil {
		fmt.Println(stat_err)
	}
	statConn.Close()
}

func reportURL(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	str := r.Form["strings"]

	statConn, err := net.Dial("tcp", "localhost:1111")
	defer statConn.Close()
	if err != nil {
		fmt.Println("Error accepting connection:", err)
		os.Exit(1)
	}

	if len(str) == 1 {
		_, stat_err := statConn.Write([]byte("2 " + str[0] + "\n"))
		if stat_err != nil {
			fmt.Println(stat_err)
		}
	} else if len(str) == 2 {
		_, stat_err := statConn.Write([]byte("2 " + str[0] + " " + str[1] + "\n"))
		if stat_err != nil {
			fmt.Println(stat_err)
		}
	} else {
		_, stat_err := statConn.Write([]byte("2 " + str[0] + " " + str[1] + " " + str[2] + "\n"))
		if stat_err != nil {
			fmt.Println(stat_err)
		}
	}

	scanner := bufio.NewScanner(statConn)
	scanner.Scan()
	response := scanner.Text()
	if response == "1" {
		jsonData, jsonErr := os.ReadFile("report.json")
		if err != nil {
			fmt.Println(jsonErr)
		}
		for _, jsonLine := range jsonData {
			fmt.Fprint(w, string(jsonLine))
		}
	}

}

func main() {
	fmt.Println("Сервис по сокращению ссылок запущен")
	http.HandleFunc("/shorten", shortenURL)
	http.HandleFunc("/", redirectURL)
	http.HandleFunc("/report", reportURL)

	http.ListenAndServe(":8080", nil)
}
