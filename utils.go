package EasyGoQ

import (
	"encoding/json"
	"strings"
)

func Dump(data interface{}) string {
	jsonData, _ := json.Marshal(data)
	return string(jsonData)
}
func ToOneLine(data string) string {
	replacer := strings.NewReplacer("\r", "", "\n", "", "  ", " ")
	data = replacer.Replace(string(data))
	return data
}
