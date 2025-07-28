package utils

import "log"

var (
	_colorPrint = false
)

func SetColorPrint(enable bool) {
	_colorPrint = enable
}

func LogInfo(format string, v ...interface{}) {
	if _colorPrint {
		log.Printf("\033[32mINFO "+format+"\033[0m\n", v...)
		return
	}
	log.Printf("INFO "+format+"\n", v...)
}

func LogWarn(format string, v ...interface{}) {
	if _colorPrint {
		log.Printf("\033[33mWARN "+format+"\033[0m\n", v...)
		return
	}
	log.Printf("WARN "+format+"\n", v...)
}

func LogErro(format string, v ...interface{}) {
	if _colorPrint {
		log.Printf("\033[31mERRO "+format+"\033[0m\n", v...)
		return
	}
	log.Printf("ERRO "+format+"\n", v...)
}

func LogFatal(format string, v ...interface{}) {
	if _colorPrint {
		log.Fatalf("\033[31mFATAL "+format+"\033[0m\n", v...)
		return
	}
	log.Fatalf("FATAL "+format+"\n", v...)
}
