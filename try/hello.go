package main

import "fmt"

func main()  {
	fmt.Println("hello")
	valueA :="hello"
	fmt.Print(valueA+"\n")
	array := [5]int {1,2,3,3,4}
	for i:=0;i<5;i++ {
		fmt.Print(array[i])
	}
}