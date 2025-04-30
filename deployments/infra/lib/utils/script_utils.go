package utils

import "fmt"

// WrapAsFile takes script content and creates a shell snippet
// that writes the content to the specified file path and makes it executable.
// It ensures the file path is quoted to handle potential spaces.
func WrapAsFile(body string, filePath string) string {
	// Use a static, predictable EOF marker. Single quotes around the marker
	// in `cat <<'MARKER'` prevent shell interpolation within the body.
	eofMarker := "EOF_SCRIPT_WRAPPER"

	return fmt.Sprintf(`
# Write script content to %s
cat <<'%s' > "%s"
%s
%s

# Make script executable
chmod +x "%s"
`, filePath, eofMarker, filePath, body, eofMarker, filePath)
}
