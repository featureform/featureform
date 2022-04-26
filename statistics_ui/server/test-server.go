package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func helloWorld(w http.ResponseWriter, req *http.Request) {
	fmt.Println(req)
	switch req.Method {
	case "GET":
		w.Write([]byte("Hello GET"))
	case "POST":
		byts, err := ioutil.ReadAll(req.Body)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(byts))
		w.Write([]byte("Hello POST"))
	}
}

func helloWorldTwo(w http.ResponseWriter, req *http.Request) {
	fmt.Println(req)
	w.Write([]byte("Hello World 2"))
}

func main_test() {
	http.HandleFunc("/", helloWorld)
	http.HandleFunc("/numbertwo", helloWorldTwo)
	http.ListenAndServe("localhost:8080", nil)
}
