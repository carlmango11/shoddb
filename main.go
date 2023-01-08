package main

import (
	"log"
	"shoddb/engine"
	"time"
)

type Person struct {
	Surname string
	Height  int
}

func main() {
	eng := engine.New[*Person]("/Users/carl/tmp/shoddb/")

	eng.Write("john1", &Person{"1", 175})
	eng.Write("john2", &Person{"2", 175})
	eng.Write("john3", &Person{"3", 175})
	eng.Write("john4", &Person{"4", 175})
	eng.Write("john5", &Person{"5", 175})
	eng.Write("john6", &Person{"6", 175})
	eng.Write("john7", &Person{"7", 175})

	john, ok := eng.Read("john1")
	log.Println(john, ok)

	time.Sleep(time.Hour)
}
