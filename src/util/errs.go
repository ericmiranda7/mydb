package util

import "log"

func Ce(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
