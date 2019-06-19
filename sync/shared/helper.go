package shared

//CloneMap clones map
func CloneMap(source map[string]interface{}) map[string]interface{} {
	var result = make(map[string]interface{})
	for k, v := range source {
		result[k] = v
	}
	return result
}
