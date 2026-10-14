package events

import (
	"strings"
	"unicode/utf8"
)

// Failure bookkeeping must itself remain writable as PostgreSQL UTF-8 text.
func boundedText(text string, maxBytes int) string {
	text = strings.ToValidUTF8(text, "�")
	text = strings.ReplaceAll(text, "\x00", "�")
	if len(text) <= maxBytes {
		return text
	}
	end := maxBytes
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end]
}
