package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
)

type KeyValuePair struct {
	key   string
	value string
}

type HashTable struct {
	hash_table [256]*KeyValuePair
}

func hash(key string) int {
	var int_key int
	for i := 0; i < len(key); i++ {
		int_key += int(key[i])
	}
	return int_key % 256
}

func (ht *HashTable) hset(key string, value string) error {
	ind := hash(key)
	node := &KeyValuePair{key: key, value: value}

	if ht.hash_table[ind] == nil {
		ht.hash_table[ind] = node
		return nil
	} else if ht.hash_table[ind].key == key {
		return errors.New("Duplicate key")
	} else {
		for i := (ind + 1) % 256; i != ind; i = (i + 1) % 256 {
			if i == ind {
				return errors.New("Hash table is full")
			}
			if ht.hash_table[i] == nil {
				ht.hash_table[i] = node
				return nil
			}
			if ht.hash_table[i].key == key {
				return errors.New("Duplicate key")
			}
		}
	}
	return errors.New("Failed to add value")
}

func (ht *HashTable) hget(key string) (string, error) {
	ind := hash(key)
	if ht.hash_table[ind] == nil {
		for i := (ind + 1) % 256; i != ind; i = (i + 1) % 256 {
			if ht.hash_table[i] != nil {
				if ht.hash_table[i].key == key {
					return ht.hash_table[i].value, nil
				}
			}
		}
		return "", errors.New("Value not found")
	} else if ht.hash_table[ind].key == key {

		return ht.hash_table[ind].value, nil
	} else {
		for i := (ind + 1) % 256; i != ind; i = (i + 1) % 256 {
			if ht.hash_table[i] != nil {
				if ht.hash_table[i].key == key {
					return ht.hash_table[i].value, nil
				}
			}
		}
	}
	return "", errors.New("Value not found")
}

func (ht *HashTable) hdel(key string) (string, error) {
	ind := hash(key)
	if ht.hash_table[ind] == nil {
		for i := (ind + 1) % 256; i != ind; i = (i + 1) % 256 {
			if ht.hash_table[i] != nil {
				if ht.hash_table[i].key == key {
					value := ht.hash_table[i].value
					ht.hash_table[i] = nil
					return value, nil
				}
			}
		}
		return "", errors.New("Element is empty")
	} else if ht.hash_table[ind].key == key {
		value := ht.hash_table[ind].value
		ht.hash_table[ind] = nil
		return value, nil
	} else {
		for i := (ind + 1) % 256; i != ind; i = (i + 1) % 256 {
			if ht.hash_table[i] != nil {
				if ht.hash_table[i].key == key {
					value := ht.hash_table[i].value
					ht.hash_table[i] = nil
					return value, nil
				}
			}
		}
	}
	return "", errors.New("Failed to delete value")
}

func (ht *HashTable) hashWriteFile(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	for i := 0; i < 256; i++ {
		if ht.hash_table[i] != nil {
			_, err = file.WriteString(ht.hash_table[i].key + " " + ht.hash_table[i].value + "\n")
			if err != nil {
				fmt.Println("Error: ", err)
			}
			_, del_err := ht.hdel(ht.hash_table[i].key)
			if del_err != nil {
				fmt.Println("Error: ", del_err)
			}
		}
	}
	return
}

func (ht *HashTable) hashReadFile(filename string) {
	read_file, read_err := os.ReadFile(filename)
	if read_err != nil {
		if os.IsNotExist(read_err) {
			create_file, create_err := os.Create(filename)
			if create_err != nil {
				panic(create_err)
			}
			create_file.Close()
			return
		}
		panic(read_err)
	}
	lines := strings.Split(string(read_file), "\n")
	for _, line := range lines {
		piece := strings.Split(line, " ")
		if len(piece) >= 2 {
			key := piece[0]
			value := strings.Join(piece[1:], " ")
			err := ht.hset(key, value)
			if err != nil {
				panic(err)
			}
		}
	}
}

func main() {
	fmt.Println("База данных запущена")
	ln, err := net.Listen("tcp", "localhost:6379") //создание tcp-сервера
	if err != nil {
		fmt.Println("Launch error:", err)
		return
	}
	defer ln.Close()

	for {
		conn, conn_err := ln.Accept() //если принят сигнал от клиента, запускается горутина
		if conn_err != nil {
			fmt.Println("Error accepting connection:", conn_err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	hashTable := HashTable{}
	scanner := bufio.NewScanner(conn)
	scanner.Scan()
	input := scanner.Text()
	parts := strings.Fields(input)
	fmt.Println(input)
	switch parts[0] {
	case "add":
		if len(parts) == 3 {

			hashTable.hashReadFile("Shorten")
			err := hashTable.hset(parts[1], parts[2])
			if err != nil {
				fmt.Println(err)
			}
			hashTable.hashWriteFile("Shorten")
			break
		}
	case "get":
		if len(parts) == 2 {

			hashTable.hashReadFile("Shorten")
			result, err := hashTable.hget(parts[1])
			if err != nil {
				fmt.Println(err)
			}
			_, send_err := conn.Write([]byte(result + "\n"))
			if send_err != nil {
				fmt.Println(send_err)
			}
			break
		}
	default:
		fmt.Println("Введена неправильная команда")
	}

}
