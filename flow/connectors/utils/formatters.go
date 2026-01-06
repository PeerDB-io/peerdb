package utils

import "fmt"

var units = []string{"B", "KB", "MB", "GB", "TB", "PB"}

// FormatTableSize converts bytes to human-readable format
func FormatTableSize(bytes int64) string {
	if bytes == 0 {
		return "0 B"
	}

	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div := int64(unit)
	exp := 1
	for n := bytes / unit; n >= unit && exp < len(units); n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %s", float64(bytes)/float64(div), units[exp])
}
